use std::fmt;

use serde::Deserialize;

use crate::pagination::page_path;
use crate::{
    ApiClient, ApiResponse, HttpError, HttpMethod, RequestOptions, client::AuthRequirement,
};
use zephyrvox_types::Snowflake;

/// Invitation resource returned by the admin API.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Invite {
    /// Invitation identifier.
    pub id: Snowflake,
    /// Role assigned when the invite is redeemed.
    pub role_key: String,
    /// Remaining redemption count.
    pub uses_left: i64,
    /// Expiry in Unix milliseconds, or `None` for no expiry.
    pub expires_at: Option<i64>,
    /// Creation time in Unix milliseconds.
    pub created_at: i64,
}

/// Request body for creating an invitation.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, Default)]
pub struct CreateInviteRequest {
    /// Role assigned to new accounts; omitted for the server default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Maximum redemption count; omitted for the server default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uses: Option<i64>,
    /// Expiry in Unix milliseconds; omitted for the server default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
}

/// Successful invitation creation response.
#[derive(Clone, PartialEq, Eq, Deserialize)]
pub struct CreateInviteResponse {
    /// Plaintext invite code. Store it only in the intended UI or vault.
    pub code: String,
    /// Persisted invitation metadata.
    pub invite: Invite,
}

impl fmt::Debug for CreateInviteResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CreateInviteResponse")
            .field("code", &"<redacted>")
            .field("invite", &self.invite)
            .finish()
    }
}

/// Process-wide UDP counters.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UdpMetrics {
    /// Received packets.
    pub packets_received: u64,
    /// All dropped packets.
    pub dropped: u64,
    /// Globally rate-limited packets.
    pub global_dropped: u64,
    /// Source-table or source-rate drops.
    pub source_dropped: u64,
    /// Per-session rate-limit drops.
    pub session_dropped: u64,
    /// Successful sends.
    pub send_success: u64,
    /// Send errors.
    pub send_errors: u64,
    /// Overall drop ratio.
    pub drop_ratio: f64,
    /// Global ingress drop ratio.
    pub global_drop_ratio: f64,
    /// Source drop ratio.
    pub source_drop_ratio: f64,
    /// Session drop ratio.
    pub session_drop_ratio: f64,
}

/// Relay queue occupancy metrics.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct QueueMetrics {
    /// Current queued item count.
    pub items: i64,
    /// Current queued byte count.
    pub bytes: i64,
    /// Item capacity.
    pub item_capacity: i64,
    /// Byte capacity.
    pub byte_capacity: i64,
    /// Overall utilization.
    pub utilization: f64,
    /// Per-shard item counts.
    pub shard_items: Vec<i64>,
    /// Per-shard byte counts.
    pub shard_bytes: Vec<i64>,
    /// Per-shard utilization.
    pub shard_utilization: Vec<f64>,
}

/// Relay counters and queue metrics.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct RelayMetrics {
    /// Frames admitted to the relay.
    pub enqueued: u64,
    /// Overload drops.
    pub dropped_overload: u64,
    /// Soft-limit drops.
    pub dropped_soft_limit: u64,
    /// Drops without a voice authority.
    pub dropped_no_authority: u64,
    /// Drops for stale authorities.
    pub dropped_stale_authority: u64,
    /// Drops due to mute policy.
    pub dropped_muted: u64,
    /// Drops without channel membership.
    pub dropped_no_membership: u64,
    /// Drops for invalid channels.
    pub dropped_invalid_channel: u64,
    /// Frames sent to recipients.
    pub sent: u64,
    /// Relay send errors.
    pub send_errors: u64,
    /// Queue occupancy.
    pub queue: QueueMetrics,
    /// P95 worker latency in milliseconds.
    pub p95_work_latency_ms: i64,
}

/// Immutable relay hard limits.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LoadHardLimits {
    /// Global packets-per-second cap.
    pub global_packets_per_sec: i64,
    /// Per-session packets-per-second cap.
    pub session_packets_per_sec: i64,
}

/// Current elastic relay soft limits.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LoadSoftLimits {
    /// Global soft packets-per-second cap.
    pub global_ingress_soft_pps: i64,
    /// Per-session soft packets-per-second cap.
    pub session_soft_pps: i64,
    /// Voice diagnostics cadence in milliseconds.
    pub voice_stats_interval_ms: i64,
}

/// Last load-controller pressure sample.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LoadInputMetrics {
    /// Relay queue utilization.
    pub queue_utilization: f64,
    /// Relay drop ratio.
    pub relay_drop_ratio: f64,
    /// UDP drop ratio.
    pub udp_drop_ratio: f64,
    /// Global UDP drop ratio.
    pub udp_global_drop_ratio: f64,
    /// Source UDP drop ratio.
    pub udp_source_drop_ratio: f64,
    /// Session UDP drop ratio.
    pub udp_session_drop_ratio: f64,
    /// Relay P95 latency in milliseconds.
    pub relay_p95_latency_ms: i64,
    /// State publication lag in milliseconds.
    pub state_publication_lag_ms: i64,
}

/// Load-controller metrics.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LoadMetrics {
    /// Immutable hard limits.
    pub hard: LoadHardLimits,
    /// Current elastic soft limits.
    pub soft: LoadSoftLimits,
    /// Whether the controller is overloaded.
    pub overloaded: bool,
    /// Number of recovery ticks.
    pub recovery_ticks: i64,
    /// Most recent pressure sample.
    pub last_input: LoadInputMetrics,
}

/// Guarded process metrics returned by `/admin/metrics`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AdminMetrics {
    /// Active ordinary HTTP connections.
    pub http_connections: i64,
    /// Active WebSocket connections.
    pub websocket_connections: i64,
    /// Active UDP voice sessions.
    pub udp_sessions: i64,
    /// UDP transport metrics.
    pub udp: UdpMetrics,
    /// Relay metrics.
    pub relay: RelayMetrics,
    /// Latest relay packet rate.
    pub relay_packets_per_second: f64,
    /// Latest relay drop ratio.
    pub relay_drop_ratio: f64,
    /// Load-controller metrics.
    pub load: LoadMetrics,
    /// State publication latency in milliseconds.
    pub state_publication_latency_ms: i64,
    /// Snapshot latency in milliseconds.
    pub snapshot_latency_ms: i64,
}

/// Typed accessor for admin invites and metrics.
pub struct AdminApi<'a> {
    client: &'a ApiClient,
}

impl ApiClient {
    /// Returns the admin resource accessor.
    pub fn admin(&self) -> AdminApi<'_> {
        AdminApi { client: self }
    }
}

impl AdminApi<'_> {
    /// Lists invitations using bounded pagination.
    pub async fn list_invites(
        &self,
        limit: u32,
        offset: u32,
    ) -> Result<ApiResponse<Vec<Invite>>, HttpError> {
        let path = page_path("admin/invites", limit, offset);
        let request = self
            .client
            .empty_request(HttpMethod::Get, &path, RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Creates an invitation and returns its plaintext code once.
    pub async fn create_invite(
        &self,
        input: CreateInviteRequest,
    ) -> Result<ApiResponse<CreateInviteResponse>, HttpError> {
        self.create_invite_with_options(input, RequestOptions::mutation())
            .await
    }

    /// Creates an invitation with a caller-selected retry identity.
    pub async fn create_invite_with_options(
        &self,
        input: CreateInviteRequest,
        mut options: RequestOptions,
    ) -> Result<ApiResponse<CreateInviteResponse>, HttpError> {
        options.ensure_idempotency();
        let request =
            self.client
                .json_request(HttpMethod::Post, "admin/invites", &input, options)?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Deletes an invitation with an automatically generated retry identity.
    pub async fn delete_invite(&self, invite_id: Snowflake) -> Result<ApiResponse<()>, HttpError> {
        self.delete_invite_with_options(invite_id, RequestOptions::mutation())
            .await
    }

    /// Deletes an invitation with a caller-selected retry identity.
    pub async fn delete_invite_with_options(
        &self,
        invite_id: Snowflake,
        mut options: RequestOptions,
    ) -> Result<ApiResponse<()>, HttpError> {
        options.ensure_idempotency();
        let path = format!("admin/invites/{invite_id}");
        let request = self
            .client
            .empty_request(HttpMethod::Delete, &path, options)?;
        self.client
            .send_no_content(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }

    /// Fetches guarded process metrics.
    pub async fn metrics(&self) -> Result<ApiResponse<AdminMetrics>, HttpError> {
        let request =
            self.client
                .empty_request(HttpMethod::Get, "admin/metrics", RequestOptions::new())?;
        self.client
            .send_json(request, AuthRequirement::AccessToken { retry_once: true })
            .await
    }
}
