import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os

logger = logging.getLogger(__name__)

class EmailChannel:
    @staticmethod
    def send(recipient: str, subject: str, body_html: str, body_text: str = None) -> bool:
        """
        Sends email via SMTP using env vars.
        Returns True if successful.
        """
        smtp_host = os.getenv("SMTP_HOST")
        smtp_port = os.getenv("SMTP_PORT", "587")
        smtp_user = os.getenv("SMTP_USER")
        smtp_pass = os.getenv("SMTP_PASS")
        sender_email = os.getenv("SMTP_FROM", "noreply@cosmicforge.bot")
        
        if not smtp_host or not smtp_user:
            logger.warning("EmailChannel: SMTP not configured. Skipping send.")
            return False # treat as failed or skipped? If skipped, maybe return True to clear queue? 
                         # Requirement says "reliability". If unconfigured, we should probably fail/dead letter it.
            return False

        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = sender_email
            msg["To"] = recipient

            part1 = MIMEText(body_text or "Please view this email in HTML capable client.", "plain")
            part2 = MIMEText(body_html, "html")
            msg.attach(part1)
            msg.attach(part2)

            with smtplib.SMTP(smtp_host, int(smtp_port)) as server:
                server.starttls()
                server.login(smtp_user, smtp_pass)
                server.sendmail(sender_email, recipient, msg.as_string())
            
            logger.info(f"EmailChannel: Sent to {recipient}")
            return True
            
        except Exception as e:
            logger.error(f"EmailChannel: Failed to send to {recipient}: {e}")
            return False
