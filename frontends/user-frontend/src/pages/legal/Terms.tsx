import { LegalPlaceholder } from "@/components/Legal/LegalPlaceholder";
import { LegalDocumentView } from "@/components/Legal/LegalDocumentView";

/** The current published Terms of Service (DRAFT banner until counsel-approved); placeholder while unpublished. */
export default function Terms() {
    return <LegalDocumentView category="TERMS_OF_SERVICE" fallback={<LegalPlaceholder title="Terms of Service" />} />;
}
