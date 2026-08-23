interface StatusBadgeProps {
    status: string | boolean | null | undefined;
    label?: string;
}

function humanizeStatus(status: string) {
    switch (status) {
        case "healthy":
            return "Healthy";
        case "partially_missing":
            return "Warning";
        case "broken":
            return "Broken";
        case "collecting_data":
            return "Collecting Data";
        case "not_ready":
            return "Not Ready";
        case "ready_for_training":
            return "Ready for Training";
        case "ready_for_shadow_deployment":
            return "Ready for Shadow Deployment";
        case "ready_for_live_promotion":
            return "Ready for Live Promotion";
        case "training_in_progress":
            return "Training In Progress";
        case "blocked":
            return "Blocked";
        case "allow":
            return "Allow";
        case "shadow":
            return "Shadow Only";
        case "block":
            return "Block";
        case "skip":
            return "Skip";
        case "active":
            return "Active";
        case "disabled":
            return "Disabled";
        case "rejected":
            return "Overfit / Rejected";
        case "rolled_back":
            return "Rolled Back";
        case "not_deployed":
            return "Not Deployed";
        case "live":
            return "Live";
        case "success":
            return "Healthy";
        case "info":
            return "Info";
        case "warning":
            return "Warning";
        case "danger":
            return "Broken";
        case "queued":
            return "Queued";
        case "running":
            return "Running";
        case "succeeded":
            return "Succeeded";
        case "failed":
            return "Failed";
        case "unsupported":
            return "Restricted";
        default:
            return status
                .replace(/_/g, " ")
                .replace(/\b\w/g, (char) => char.toUpperCase());
    }
}

function badgeClass(status: string | boolean | null | undefined) {
    if (typeof status === "boolean") {
        return status ? "admin-badge-success" : "admin-badge-warning";
    }

    switch (status) {
        case "healthy":
        case "ready_for_training":
        case "ready_for_live_promotion":
        case "live":
        case "active":
        case "allow":
        case "success":
        case "succeeded":
            return "admin-badge-success";
        case "shadow":
        case "ready_for_shadow_deployment":
        case "not_deployed":
        case "info":
        case "queued":
        case "running":
            return "admin-badge-info";
        case "skip":
        case "partially_missing":
        case "collecting_data":
        case "training_in_progress":
        case "rolled_back":
        case "disabled":
        case "warning":
        case "unsupported":
            return "admin-badge-warning";
        case "block":
        case "broken":
        case "blocked":
        case "rejected":
        case "not_ready":
        case "danger":
        case "failed":
            return "admin-badge-danger";
        default:
            return "admin-badge-info";
    }
}

export function StatusBadge({ status, label }: StatusBadgeProps) {
    const normalized = typeof status === "string" ? status.toLowerCase() : status;
    const content =
        label
        || (typeof normalized === "boolean" ? (normalized ? "Healthy" : "Warning") : humanizeStatus(normalized || "unknown"));

    return <span className={`admin-badge ${badgeClass(normalized)}`}>{content}</span>;
}
