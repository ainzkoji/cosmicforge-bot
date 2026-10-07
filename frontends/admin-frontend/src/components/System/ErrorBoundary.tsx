import { Component } from "react";
import type { ErrorInfo, ReactNode } from "react";

interface ErrorBoundaryProps {
    children: ReactNode;
}

interface ErrorBoundaryState {
    hasError: boolean;
}

/**
 * Top-level error boundary: if rendering throws, show a plain fallback with a
 * reload button instead of a blank page.
 */
export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
    state: ErrorBoundaryState = { hasError: false };

    static getDerivedStateFromError(): ErrorBoundaryState {
        return { hasError: true };
    }

    componentDidCatch(error: Error, info: ErrorInfo): void {
        console.error("Unhandled UI error:", error, info.componentStack);
    }

    render(): ReactNode {
        if (!this.state.hasError) {
            return this.props.children;
        }
        return (
            <div
                role="alert"
                style={{
                    minHeight: "100vh",
                    display: "flex",
                    flexDirection: "column",
                    alignItems: "center",
                    justifyContent: "center",
                    gap: "16px",
                    padding: "24px",
                    textAlign: "center",
                    fontFamily: "system-ui, sans-serif",
                    background: "#0F1218",
                    color: "#E5E7EB",
                }}
            >
                <h1 style={{ fontSize: "24px", fontWeight: 700 }}>Something went wrong</h1>
                <p style={{ maxWidth: "480px", color: "#9CA3AF" }}>
                    This admin page could not be displayed. Reloading usually fixes it. Trading is not affected
                    by this screen.
                </p>
                <button
                    type="button"
                    onClick={() => window.location.reload()}
                    style={{
                        padding: "10px 20px",
                        borderRadius: "8px",
                        border: "none",
                        background: "#4F46E5",
                        color: "#FFFFFF",
                        fontWeight: 700,
                        cursor: "pointer",
                    }}
                >
                    Reload
                </button>
            </div>
        );
    }
}
