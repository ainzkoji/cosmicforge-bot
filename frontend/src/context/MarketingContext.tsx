import { createContext, useContext, useEffect, useState, ReactNode } from "react";
import { api } from "@/api/client";
import { useSearchParams } from "react-router-dom";

interface MarketingContextType {
    sessionId: string | null;
    trackEvent: (eventType: string, page: string, metadata?: any) => void;
    createPricingIntent: (planId: string) => Promise<string | null>;
}

const MarketingContext = createContext<MarketingContextType | null>(null);

const STORAGE_KEY = "cosmic_marketing_sid";

export function MarketingProvider({ children }: { children: ReactNode }) {
    const [sessionId, setSessionId] = useState<string | null>(null);
    const [searchParams] = useSearchParams();

    // Initialize tracking session
    useEffect(() => {
        const initSession = async () => {
            // Check if we already have a session
            let sid = localStorage.getItem(STORAGE_KEY);

            // If manual ref/utm params exist, we might want to start a new session or update?
            // For now, simpler: if no session, create one.
            if (!sid) {
                try {
                    const params: any = {
                        landing_page: window.location.pathname
                    };

                    // Harvest UTMs
                    ['utm_source', 'utm_medium', 'utm_campaign', 'utm_content', 'utm_term', 'ref', 'aff_broker'].forEach(key => {
                        const val = searchParams.get(key);
                        if (val) {
                            if (key === 'ref') params.ref_code = val;
                            else if (key === 'aff_broker') params.aff_broker = val;
                            else params[key] = val; // utm keys match interface
                        }
                    });

                    const res = await api.createMarketingSession(params);
                    sid = res.session_id;
                    localStorage.setItem(STORAGE_KEY, sid);
                } catch (err) {
                    console.error("Failed to init marketing session", err);
                    return; // Fail gracefully
                }
            }

            setSessionId(sid);

            // Track page view
            api.trackEvent({
                session_id: sid,
                event_type: "page_view",
                page: window.location.pathname
            });
        };

        initSession();
    }, [searchParams]);

    const trackEvent = (eventType: string, page: string, metadata?: any) => {
        if (!sessionId) return;
        api.trackEvent({
            session_id: sessionId,
            event_type: eventType,
            page: page,
            metadata
        });
    };

    const createPricingIntent = async (planId: string) => {
        if (!sessionId) return null;
        try {
            const res = await api.createPricingIntent({
                marketing_session_id: sessionId,
                plan_id: planId
            });
            return res.intent_id;
        } catch (err) {
            console.error(err);
            return null;
        }
    };

    return (
        <MarketingContext.Provider value={{ sessionId, trackEvent, createPricingIntent }}>
            {children}
        </MarketingContext.Provider>
    );
}

export const useMarketing = () => {
    const context = useContext(MarketingContext);
    if (!context) throw new Error("useMarketing must be used within MarketingProvider");
    return context;
};
