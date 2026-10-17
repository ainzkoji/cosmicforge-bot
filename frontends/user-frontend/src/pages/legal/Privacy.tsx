import { LegalPlaceholder } from "@/components/Legal/LegalPlaceholder";
import { LegalDocumentView } from "@/components/Legal/LegalDocumentView";

/** The current published Privacy Policy (DRAFT banner until counsel-approved); placeholder while unpublished. */
export default function Privacy() {
    return <LegalDocumentView category="PRIVACY_POLICY" fallback={<LegalPlaceholder title="Privacy Policy" />} />;
}
