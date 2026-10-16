use crate::EnrichmentStatus;

pub fn may_schedule_enrichment(status: EnrichmentStatus) -> bool {
    matches!(
        status,
        EnrichmentStatus::Discovered | EnrichmentStatus::Failed
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enrichment_eligible_statuses() {
        assert!(may_schedule_enrichment(EnrichmentStatus::Discovered));
        assert!(may_schedule_enrichment(EnrichmentStatus::Failed));
        assert!(!may_schedule_enrichment(EnrichmentStatus::Enriched));
        assert!(!may_schedule_enrichment(EnrichmentStatus::Blocked));
    }
}
