#![deny(unsafe_code)]
#![deny(clippy::all)]
#![deny(unreachable_pub)]
#![warn(missing_docs)]

//! Session capsule for Astrid OS.
//!
//! Dumb, trustworthy store for conversation history. Holds clean messages:
//! what the user said, what the assistant replied, what tools returned.
//! Never transforms anything. Clean in, clean out.
//!
//! The react loop (or any future replacement) appends messages at turn
//! boundaries and fetches history when building LLM requests. Prompt
//! builder injections, system prompt assembly, context compaction -
//! those are ephemeral per-turn transforms that never touch session.
//!
//! # Session chaining
//!
//! Sessions form a linked list via `parent_session_id`. When a session
//! is cleared or compacted, a new session is created pointing back to
//! the old one. History is never silently truncated.

use astrid_sdk::prelude::*;
use astrid_sdk::types::{ContentPart, Message, MessageContent, MessageRole};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// KV key prefix for session data.
const SESSION_KEY_PREFIX: &str = "session.data";

/// Default session ID.
const DEFAULT_SESSION_ID: &str = "default";

/// Current schema version for `SessionData`.
///
/// - v0: pre-versioning (only `messages`).
/// - v1: added `schema_version` + `parent_session_id`.
/// - v2: added `created_at` / `updated_at` timestamps.
const SESSION_DATA_SCHEMA_VERSION: u32 = 2;

/// Default page size for `handle_list` when the request omits `limit`.
const DEFAULT_LIST_LIMIT: u32 = 50;

/// Hard cap on the `handle_list` page size, mirrored by the gateway. Bounds
/// the per-request KV reads (one blob load per listed session).
const MAX_LIST_LIMIT: u32 = 200;

/// Maximum length, in characters, of the session preview snippet.
const PREVIEW_MAX_CHARS: usize = 80;

/// Maximum number of CAS retry attempts before giving up.
///
/// Concurrent writers to the same session ID race on `kv::cas`. Eight
/// retries is generous for the realistic concurrency level (a single
/// react loop per session) and bounds worst-case latency under
/// adversarial contention.
const CAS_RETRY_LIMIT: u32 = 8;

/// Build the KV key for a session's data.
fn session_key(session_id: &str) -> String {
    format!("{SESSION_KEY_PREFIX}.{session_id}")
}

/// The KV key prefix, including the trailing separator, shared by every
/// session data key. Used to enumerate the principal's sessions via
/// [`kv::list_keys_page`].
fn session_key_prefix() -> String {
    format!("{SESSION_KEY_PREFIX}.")
}

/// Current wall-clock time as Unix epoch seconds, or `None` if the host
/// clock is unavailable. Timestamps are best-effort: a missing clock leaves
/// them unset rather than failing the write.
fn now_unix() -> Option<u64> {
    astrid_sdk::time::now()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs())
}

/// Derive a short preview from the first user message in `messages`,
/// truncated to [`PREVIEW_MAX_CHARS`] characters. Returns `None` when there
/// is no user message carrying extractable text (e.g. only tool traffic).
fn session_preview(messages: &[Message]) -> Option<String> {
    let first_user = messages.iter().find(|m| m.role == MessageRole::User)?;
    let text = match &first_user.content {
        MessageContent::Text(s) => s.as_str(),
        MessageContent::MultiPart(parts) => parts.iter().find_map(|p| match p {
            ContentPart::Text { text } => Some(text.as_str()),
            ContentPart::Image { .. } => None,
        })?,
        MessageContent::ToolCalls(_) | MessageContent::ToolResult(_) => return None,
    };
    Some(truncate_chars(text.trim(), PREVIEW_MAX_CHARS))
}

/// Truncate `s` to at most `max` characters on a char boundary, appending an
/// ellipsis if any characters were dropped.
fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max).collect();
    out.push('…');
    out
}

/// Build the JSON metadata summary for one session (no transcript body).
fn session_summary_json(session_id: &str, data: &SessionData) -> serde_json::Value {
    serde_json::json!({
        "session_id": session_id,
        "message_count": data.messages.len(),
        "created_at": data.created_at,
        "updated_at": data.updated_at,
        "parent_session_id": data.parent_session_id,
        "preview": session_preview(&data.messages),
    })
}

/// Persistent conversation session data.
///
/// Schema-versioned for forward-compatible deserialization. On load
/// ([`SessionData::load`]) the blob self-heals to the current schema:
/// - v0 (legacy, no version field) and v1 (no timestamps): stamp to the
///   current version and re-save via best-effort CAS. Timestamps stay `None`
///   (genuinely unknown for pre-v2 data) and populate on the next write.
/// - current version: use as-is.
/// - Unknown future version: log error, start fresh (fail secure).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SessionData {
    /// Schema version. Defaults to 0 for pre-versioning data.
    #[serde(default)]
    schema_version: u32,
    /// Previous session in the chain, if this session was created
    /// via clear or compaction.
    #[serde(default)]
    parent_session_id: Option<String>,
    /// Unix epoch seconds when this session was first written. `None` for
    /// pre-v2 sessions (genuinely unknown); populated on the next write.
    #[serde(default)]
    created_at: Option<u64>,
    /// Unix epoch seconds of the most recent write (append or clear). `None`
    /// for pre-v2 sessions until their next write.
    #[serde(default)]
    updated_at: Option<u64>,
    /// Clean conversation message history.
    messages: Vec<Message>,
}

impl Default for SessionData {
    fn default() -> Self {
        Self {
            schema_version: SESSION_DATA_SCHEMA_VERSION,
            parent_session_id: None,
            created_at: None,
            updated_at: None,
            messages: Vec::new(),
        }
    }
}

impl SessionData {
    /// Apply schema migration to deserialized data (pure logic, no I/O).
    ///
    /// Returns `Ok((data, needs_save))` if migration succeeded or was
    /// unnecessary. `needs_save` is true when the version was bumped.
    /// Returns `Err(fresh_default)` if the version is unrecognized
    /// (fail secure).
    fn migrate(mut self) -> Result<(Self, bool), Self> {
        match self.schema_version {
            // v0 (pre-versioning) and v1 (pre-timestamps) both upgrade to the
            // current version by stamping it. Timestamps are left None —
            // genuinely unknown for pre-v2 data — and populate on next write.
            v if v < SESSION_DATA_SCHEMA_VERSION => {
                self.schema_version = SESSION_DATA_SCHEMA_VERSION;
                Ok((self, true))
            }
            v if v == SESSION_DATA_SCHEMA_VERSION => Ok((self, false)),
            _ => Err(Self::default()),
        }
    }

    /// Stamp timestamps for a write at `now` (Unix epoch seconds): sets
    /// `created_at` on the first write to this session and always refreshes
    /// `updated_at`.
    fn touch(&mut self, now: u64) {
        self.created_at.get_or_insert(now);
        self.updated_at = Some(now);
    }

    /// Load session data from KV, applying schema migration as needed.
    ///
    /// Returns `(data, raw_bytes)` where `raw_bytes` is the exact KV
    /// payload that was read (or `None` for a missing key). Callers use
    /// `raw_bytes` as the `expected` value in [`kv::cas`] to detect
    /// concurrent writers. Returns `Self::default()` with `raw_bytes =
    /// None` if the key is absent.
    fn load(session_id: &str) -> (Self, Option<Vec<u8>>) {
        let key = session_key(session_id);
        let raw = match kv::get_bytes_opt(&key) {
            Ok(b) => b,
            Err(e) => {
                log::error(format!("Failed to load session bytes, starting fresh: {e}"));
                return (Self::default(), None);
            }
        };

        let Some(bytes) = raw else {
            // Key is absent: fresh default with no expected bytes (CAS
            // will use `expected = None` for create-if-absent semantics).
            return (Self::default(), None);
        };

        let data = serde_json::from_slice::<Self>(&bytes).unwrap_or_else(|e| {
            log::error(format!("Failed to parse session data, starting fresh: {e}"));
            Self::default()
        });

        match data.migrate() {
            Ok((migrated, needs_save)) => {
                // If version was bumped (v0 -> v1), persist the migration
                // via CAS so we don't clobber a concurrent writer. Best-
                // effort: in-memory data is usable either way, and the
                // next modification will retry the migration if this one
                // races out.
                if needs_save {
                    let migrated_bytes = match serde_json::to_vec(&migrated) {
                        Ok(b) => b,
                        Err(e) => {
                            log::warn(format!("Failed to serialize migrated session: {e}"));
                            return (migrated, Some(bytes));
                        }
                    };
                    match kv::cas(&key, Some(&bytes), &migrated_bytes) {
                        Ok(true) => (migrated, Some(migrated_bytes)),
                        Ok(false) => {
                            // Lost the race to another writer; their value
                            // is now authoritative. Caller will pick it up
                            // on its own retry loop.
                            log::debug(format!(
                                "session '{session_id}' migration CAS lost \
                                 race; another writer migrated first"
                            ));
                            (migrated, Some(bytes))
                        }
                        Err(e) => {
                            log::warn(format!("Failed to CAS-save session after migration: {e}"));
                            (migrated, Some(bytes))
                        }
                    }
                } else {
                    (migrated, Some(bytes))
                }
            }
            Err(fresh) => {
                log::error(format!(
                    "Session '{session_id}' has unknown schema version \
                         (expected {SESSION_DATA_SCHEMA_VERSION}), starting fresh"
                ));
                (fresh, Some(bytes))
            }
        }
    }

    /// Atomically read-modify-write session data using [`kv::cas`].
    ///
    /// Retries up to [`CAS_RETRY_LIMIT`] times if a concurrent writer
    /// wins the swap. The `mutate` closure is called fresh on every
    /// retry so logic that derives state (e.g. truncation, dedup) sees
    /// the current stored value. Returns the post-write [`SessionData`]
    /// so callers that need the merged history (e.g. append-before-read)
    /// avoid a second `load`.
    fn modify_atomic<F>(session_id: &str, mut mutate: F) -> Result<Self, SysError>
    where
        F: FnMut(&mut Self),
    {
        let key = session_key(session_id);

        for attempt in 0..CAS_RETRY_LIMIT {
            let (mut data, expected) = Self::load(session_id);
            mutate(&mut data);
            let new_bytes = serde_json::to_vec(&data)?;
            let expected_slice = expected.as_deref();

            if kv::cas(&key, expected_slice, &new_bytes)? {
                return Ok(data);
            }

            log::debug(format!(
                "session '{session_id}' CAS attempt {} lost race; retrying",
                attempt + 1
            ));
        }

        Err(SysError::ApiError(format!(
            "session '{session_id}' write contended for {CAS_RETRY_LIMIT} attempts; \
             giving up to avoid unbounded retry"
        )))
    }
}

/// Session capsule. Dumb store with session chaining.
///
/// # Security note
///
/// Session isolation (restricting which capsules can read/write which
/// session IDs) is enforced at the kernel's topic ACL layer, not within
/// this capsule. Any capsule with `ipc_publish` permission for the
/// `session.request.*` topics can access any session by ID.
#[derive(Default)]
pub struct Session;

#[capsule]
impl Session {
    /// Handles `session.append` events.
    ///
    /// Appends one or more messages to the conversation history.
    /// Fire-and-forget - no response published.
    ///
    /// The react capsule uses `append_before_read` on `get_messages` for
    /// atomic appends. This standalone handler exists as a public API for
    /// other capsules that need to inject messages without reading history
    /// (e.g. system notifications, external integrations).
    #[astrid::interceptor("handle_append")]
    pub fn handle_append(&self, payload: serde_json::Value) -> Result<(), SysError> {
        let session_id = payload
            .get("session_id")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_SESSION_ID);

        let messages: Vec<Message> = payload
            .get("messages")
            .cloned()
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| SysError::ApiError(format!("Failed to parse messages: {e}")))?
            .unwrap_or_default();

        if messages.is_empty() {
            return Ok(());
        }

        // Atomic append: `kv::cas` guarantees we never clobber a
        // concurrent writer's appends. The closure is re-run on each
        // retry against the freshly-loaded value, so all messages from
        // both writers end up in the final list.
        let now = now_unix();
        SessionData::modify_atomic(session_id, |data| {
            data.messages.extend(messages.iter().cloned());
            if let Some(now) = now {
                data.touch(now);
            }
        })
        .map(|_| ())
    }

    /// Extracts and validates `correlation_id` from a request payload.
    ///
    /// The correlation_id is interpolated into per-request scoped reply
    /// topics as a single dot-separated segment. Rejects empty values and
    /// values containing dots (which would add extra segments, breaking
    /// the ACL pattern match).
    fn require_correlation_id<'a>(
        payload: &'a serde_json::Value,
        request_name: &str,
    ) -> Result<&'a str, SysError> {
        payload
            .get("correlation_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty() && !s.contains('.'))
            .ok_or_else(|| {
                SysError::ApiError(format!(
                    "{request_name} request missing or invalid correlation_id \
                     (must be non-empty, no dots)"
                ))
            })
    }

    /// Handles `session.request.get_messages` events.
    ///
    /// Returns the conversation history to the requester via a per-request
    /// scoped reply topic (`session.v1.response.get_messages.<correlation_id>`).
    /// This prevents cross-instance response theft under concurrent load.
    ///
    /// Supports an optional `append_before_read` field containing messages
    /// to append atomically before returning the history. This eliminates
    /// the race between a separate `session.append` and `get_messages`.
    #[astrid::interceptor("handle_get_messages")]
    pub fn handle_get_messages(&self, payload: serde_json::Value) -> Result<(), SysError> {
        let session_id = payload
            .get("session_id")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_SESSION_ID);

        let correlation_id = Self::require_correlation_id(&payload, "get_messages")?;

        // Atomic append-before-read: if the requester provides messages to
        // append, store them first so the returned history includes them.
        // `kv::cas` makes the read-modify-write race-free against any
        // concurrent `handle_append` on the same session.
        let data = if let Some(append_msgs) = payload.get("append_before_read").cloned() {
            let msgs: Vec<Message> = serde_json::from_value(append_msgs).map_err(|e| {
                SysError::ApiError(format!("Failed to parse append_before_read: {e}"))
            })?;
            if msgs.is_empty() {
                SessionData::load(session_id).0
            } else {
                let now = now_unix();
                SessionData::modify_atomic(session_id, |data| {
                    data.messages.extend(msgs.iter().cloned());
                    if let Some(now) = now {
                        data.touch(now);
                    }
                })?
            }
        } else {
            SessionData::load(session_id).0
        };

        // correlation_id is redundant with the scoped topic but retained
        // in the payload for observability (log inspection, debugging).
        let reply_topic = format!("session.v1.response.get_messages.{correlation_id}");
        ipc::publish_json(
            &reply_topic,
            &serde_json::json!({
                "correlation_id": correlation_id,
                "messages": data.messages,
            }),
        )
    }

    /// Handles `session.v1.request.clear` events.
    ///
    /// Creates a new session with `parent_session_id` pointing to the
    /// old one. The old session's data is left intact in KV for history
    /// traversal. Returns the new session ID via a per-request scoped
    /// reply topic (`session.v1.response.clear.<correlation_id>`).
    #[astrid::interceptor("handle_clear")]
    pub fn handle_clear(&self, payload: serde_json::Value) -> Result<(), SysError> {
        let old_session_id = payload
            .get("session_id")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_SESSION_ID);

        let correlation_id = Self::require_correlation_id(&payload, "clear")?;

        let new_session_id = Uuid::new_v4().to_string();

        let now = now_unix();
        let new_data = SessionData {
            schema_version: SESSION_DATA_SCHEMA_VERSION,
            parent_session_id: Some(old_session_id.to_string()),
            created_at: now,
            updated_at: now,
            messages: Vec::new(),
        };
        let new_bytes = serde_json::to_vec(&new_data)?;
        // Create-if-absent: a UUID v4 collision with an existing session
        // key is astronomically unlikely but still fail-secure rather
        // than silently overwrite. `cas(key, None, ...)` returns false
        // if the key already exists.
        let created = kv::cas(&session_key(&new_session_id), None, &new_bytes)?;
        if !created {
            return Err(SysError::ApiError(format!(
                "session '{new_session_id}' UUID collision detected; \
                 refusing to overwrite existing session"
            )));
        }

        log::info(format!(
            "Session cleared: '{old_session_id}' -> '{new_session_id}' \
                 (old session preserved)"
        ));

        let reply_topic = format!("session.v1.response.clear.{correlation_id}");
        ipc::publish_json(
            &reply_topic,
            &serde_json::json!({
                "correlation_id": correlation_id,
                "new_session_id": new_session_id,
                "old_session_id": old_session_id,
            }),
        )
    }

    /// Handles `session.v1.request.list` events.
    ///
    /// Enumerates the invoking principal's sessions and returns a paginated
    /// page of metadata summaries (id, message count, timestamps, parent,
    /// preview — no transcript bodies) via the per-request scoped reply topic
    /// `session.v1.response.list.<correlation_id>`.
    ///
    /// Pagination follows the KV key cursor: pages are ordered by session
    /// key, and `next_cursor` is the opaque cursor for the following page (or
    /// absent on the last page). Each summary carries `updated_at` so callers
    /// can present threads by recency.
    ///
    /// # Per-principal scope
    ///
    /// The kernel scopes this capsule's KV namespace to the invoking
    /// principal, so [`kv::list_keys_page`] only ever returns the caller's
    /// own session keys. There is no cross-principal enumeration path: a
    /// caller cannot observe another principal's threads, even by id.
    #[astrid::interceptor("handle_list")]
    pub fn handle_list(&self, payload: serde_json::Value) -> Result<(), SysError> {
        let correlation_id = Self::require_correlation_id(&payload, "list")?;

        let cursor = payload.get("cursor").and_then(|v| v.as_str());
        let limit = payload
            .get("limit")
            .and_then(serde_json::Value::as_u64)
            .map(|l| l.clamp(1, u64::from(MAX_LIST_LIMIT)) as u32)
            .unwrap_or(DEFAULT_LIST_LIMIT);

        let prefix = session_key_prefix();
        let page = kv::list_keys_page(&prefix, cursor, limit)?;

        let mut sessions = Vec::with_capacity(page.keys.len());
        for key in &page.keys {
            // `list_keys_page` returns full KV keys; strip the prefix back to
            // the session id. A non-matching key can't occur (we queried by
            // this prefix) but is skipped defensively.
            let Some(session_id) = key.strip_prefix(&prefix) else {
                continue;
            };
            // `load` self-heals each blob's schema as a side effect.
            let (data, _) = SessionData::load(session_id);
            sessions.push(session_summary_json(session_id, &data));
        }

        let reply_topic = format!("session.v1.response.list.{correlation_id}");
        ipc::publish_json(
            &reply_topic,
            &serde_json::json!({
                "correlation_id": correlation_id,
                "sessions": sessions,
                "next_cursor": page.next_cursor,
            }),
        )
    }
}

// ---------------------------------------------------------------------------
// Tests (serde-level, no host functions)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Pre-versioning (v0) data with only `messages` deserializes correctly.
    /// The `schema_version` defaults to 0 and `parent_session_id` defaults to None.
    #[test]
    fn test_session_data_v0_backward_compat() {
        let v0_json = r#"{"messages":[]}"#;
        let data: SessionData = serde_json::from_str(v0_json).unwrap();
        assert_eq!(data.schema_version, 0);
        assert!(data.parent_session_id.is_none());
        assert!(data.messages.is_empty());
    }

    /// v1 data round-trips through serde (migration is applied by `load`,
    /// not by serde, so the version is preserved verbatim here).
    #[test]
    fn test_session_data_v1_round_trip() {
        let data = SessionData {
            schema_version: 1,
            parent_session_id: Some("old-session-abc".into()),
            created_at: None,
            updated_at: None,
            messages: Vec::new(),
        };
        let json = serde_json::to_string(&data).unwrap();
        let loaded: SessionData = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.schema_version, 1);
        assert_eq!(loaded.parent_session_id.as_deref(), Some("old-session-abc"));
    }

    /// A v1-shaped blob (no timestamp fields) deserializes with
    /// `created_at`/`updated_at` defaulting to `None`.
    #[test]
    fn test_session_data_v1_json_defaults_timestamps_none() {
        let v1_json = r#"{"schema_version":1,"parent_session_id":null,"messages":[]}"#;
        let data: SessionData = serde_json::from_str(v1_json).unwrap();
        assert_eq!(data.schema_version, 1);
        assert!(data.created_at.is_none());
        assert!(data.updated_at.is_none());
    }

    /// v2 data with timestamps round-trips correctly.
    #[test]
    fn test_session_data_v2_round_trip() {
        let data = SessionData {
            schema_version: 2,
            parent_session_id: None,
            created_at: Some(1_719_000_000),
            updated_at: Some(1_719_000_100),
            messages: Vec::new(),
        };
        let json = serde_json::to_string(&data).unwrap();
        let loaded: SessionData = serde_json::from_str(&json).unwrap();
        assert_eq!(loaded.schema_version, 2);
        assert_eq!(loaded.created_at, Some(1_719_000_000));
        assert_eq!(loaded.updated_at, Some(1_719_000_100));
    }

    /// Unknown future version deserializes without error (serde doesn't
    /// know about version semantics - the `load()` function handles that).
    #[test]
    fn test_session_data_future_version_deserializes() {
        let future_json = r#"{"schema_version":99,"messages":[],"extra_field":"ignored"}"#;
        let data: SessionData = serde_json::from_str(future_json).unwrap();
        assert_eq!(data.schema_version, 99);
        assert!(data.messages.is_empty());
    }

    /// Default SessionData has the current schema version.
    #[test]
    fn test_session_data_default_has_current_version() {
        let data = SessionData::default();
        assert_eq!(data.schema_version, SESSION_DATA_SCHEMA_VERSION);
        assert!(data.parent_session_id.is_none());
    }

    /// v0 data migrates to current version and signals needs_save.
    #[test]
    fn test_migrate_v0_stamps_to_current() {
        let v0_json = r#"{"messages":[]}"#;
        let data: SessionData = serde_json::from_str(v0_json).unwrap();
        assert_eq!(data.schema_version, 0);

        let (migrated, needs_save) = data.migrate().expect("v0 should migrate successfully");
        assert_eq!(migrated.schema_version, SESSION_DATA_SCHEMA_VERSION);
        assert!(needs_save, "v0 -> v1 migration should signal needs_save");
        assert!(migrated.messages.is_empty());
    }

    /// Current version data passes through migrate unchanged.
    #[test]
    fn test_migrate_current_version_is_noop() {
        let data = SessionData::default();
        let (migrated, needs_save) = data.migrate().expect("current version should pass");
        assert_eq!(migrated.schema_version, SESSION_DATA_SCHEMA_VERSION);
        assert!(!needs_save, "current version should not signal needs_save");
    }

    /// Unknown future version fails migration (fail secure).
    #[test]
    fn test_migrate_unknown_version_fails_secure() {
        let data = SessionData {
            schema_version: 99,
            parent_session_id: Some("old".into()),
            created_at: Some(1),
            updated_at: Some(2),
            messages: Vec::new(),
        };
        let fresh = data
            .migrate()
            .expect_err("unknown version should fail migration");
        assert_eq!(fresh.schema_version, SESSION_DATA_SCHEMA_VERSION);
        assert!(
            fresh.parent_session_id.is_none(),
            "fresh default has no parent"
        );
    }

    /// v1 data (no timestamps) migrates to the current version, signals
    /// needs_save, and leaves timestamps unset.
    #[test]
    fn test_migrate_v1_to_current_leaves_timestamps_none() {
        let v1_json = r#"{"schema_version":1,"parent_session_id":null,"messages":[]}"#;
        let data: SessionData = serde_json::from_str(v1_json).unwrap();
        assert_eq!(data.schema_version, 1);

        let (migrated, needs_save) = data.migrate().expect("v1 should migrate");
        assert_eq!(migrated.schema_version, SESSION_DATA_SCHEMA_VERSION);
        assert!(
            needs_save,
            "v1 -> current migration should signal needs_save"
        );
        assert!(migrated.created_at.is_none());
        assert!(migrated.updated_at.is_none());
    }

    /// `touch` stamps `created_at` once (first write) and always refreshes
    /// `updated_at`.
    #[test]
    fn test_touch_sets_created_once_updates_updated() {
        let mut data = SessionData::default();
        assert!(data.created_at.is_none());

        data.touch(100);
        assert_eq!(data.created_at, Some(100));
        assert_eq!(data.updated_at, Some(100));

        data.touch(200);
        assert_eq!(data.created_at, Some(100), "created_at must not move");
        assert_eq!(data.updated_at, Some(200), "updated_at tracks last write");
    }

    // -- preview extraction --

    #[test]
    fn test_preview_first_user_message() {
        let messages = vec![
            Message::system("you are helpful"),
            Message::user("what is the capital of France?"),
            Message::assistant("Paris."),
        ];
        assert_eq!(
            session_preview(&messages).as_deref(),
            Some("what is the capital of France?")
        );
    }

    #[test]
    fn test_preview_none_without_user_message() {
        let messages = vec![Message::system("sys"), Message::assistant("hi")];
        assert!(session_preview(&messages).is_none());
    }

    #[test]
    fn test_preview_empty_history() {
        assert!(session_preview(&[]).is_none());
    }

    #[test]
    fn test_preview_multipart_extracts_text() {
        let messages = vec![Message {
            role: MessageRole::User,
            content: MessageContent::MultiPart(vec![
                ContentPart::Image {
                    data: "base64".into(),
                    media_type: "image/png".into(),
                },
                ContentPart::Text {
                    text: "describe this".into(),
                },
            ]),
        }];
        assert_eq!(session_preview(&messages).as_deref(), Some("describe this"));
    }

    #[test]
    fn test_preview_truncates_long_text() {
        let long = "x".repeat(200);
        let messages = vec![Message::user(long)];
        let preview = session_preview(&messages).expect("preview present");
        // PREVIEW_MAX_CHARS chars plus a one-char ellipsis.
        assert_eq!(preview.chars().count(), PREVIEW_MAX_CHARS + 1);
        assert!(preview.ends_with('…'));
    }

    #[test]
    fn test_truncate_chars_is_char_boundary_safe() {
        // Multi-byte characters must not be split mid-codepoint.
        let s = "é".repeat(100);
        let out = truncate_chars(&s, 10);
        assert_eq!(out.chars().count(), 11); // 10 + ellipsis
        assert!(out.ends_with('…'));
    }

    #[test]
    fn test_session_summary_json_shape() {
        let data = SessionData {
            schema_version: 2,
            parent_session_id: Some("parent-1".into()),
            created_at: Some(10),
            updated_at: Some(20),
            messages: vec![Message::user("hello"), Message::assistant("hi")],
        };
        let summary = session_summary_json("sess-1", &data);
        assert_eq!(summary["session_id"], "sess-1");
        assert_eq!(summary["message_count"], 2);
        assert_eq!(summary["created_at"], 10);
        assert_eq!(summary["updated_at"], 20);
        assert_eq!(summary["parent_session_id"], "parent-1");
        assert_eq!(summary["preview"], "hello");
    }

    /// v0 data with existing messages preserves them through migration.
    #[test]
    fn test_migrate_v0_preserves_messages() {
        let v0_json = r#"{"messages":[{"role":"user","content":"hello"}]}"#;
        let data: SessionData = serde_json::from_str(v0_json).unwrap();
        assert_eq!(data.schema_version, 0);
        assert_eq!(data.messages.len(), 1);

        let (migrated, _) = data.migrate().expect("v0 should migrate");
        assert_eq!(migrated.messages.len(), 1);
    }

    /// v0 data with parent_session_id preserves it through migration.
    #[test]
    fn test_migrate_v0_preserves_parent() {
        let v0_json = r#"{"messages":[],"parent_session_id":"parent-abc"}"#;
        let data: SessionData = serde_json::from_str(v0_json).unwrap();
        let (migrated, _) = data.migrate().expect("v0 should migrate");
        assert_eq!(migrated.parent_session_id.as_deref(), Some("parent-abc"));
    }

    // -- correlation_id validation (scoped reply topic safety) --
    // Tests exercise Session::require_correlation_id directly.

    #[test]
    fn test_correlation_id_rejects_empty() {
        let payload = serde_json::json!({ "correlation_id": "" });
        assert!(Session::require_correlation_id(&payload, "test").is_err());
    }

    #[test]
    fn test_correlation_id_rejects_missing() {
        let payload = serde_json::json!({});
        assert!(Session::require_correlation_id(&payload, "test").is_err());
    }

    #[test]
    fn test_correlation_id_rejects_dots() {
        let payload = serde_json::json!({ "correlation_id": "abc.def" });
        assert!(Session::require_correlation_id(&payload, "test").is_err());
    }

    #[test]
    fn test_correlation_id_accepts_uuid() {
        let payload =
            serde_json::json!({ "correlation_id": "550e8400-e29b-41d4-a716-446655440000" });
        assert_eq!(
            Session::require_correlation_id(&payload, "test").unwrap(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }
}
