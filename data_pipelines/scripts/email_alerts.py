import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from jinja2 import Template
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import logging

logger = logging.getLogger(__name__)

def _to_list(s: str):
    return [e.strip() for e in s.split(",") if e.strip()]

def _get_recipients():
    try:
        return _to_list(Variable.get("ALERT_TO"))
    except Exception:
        return []

def _get_smtp_connection():
    conn = BaseHook.get_connection("email_smtp")
    host = conn.host or "smtp.gmail.com"
    port = int(conn.port or 465)
    user = conn.login
    password = conn.password
    use_ssl = port == 465
    return host, port, user, password, use_ssl

def _send_email(subject: str, html: str):
    recipients = _get_recipients()
    if not recipients:
        logger.warning("No recipients found in ALERT_TO variable.")
        return

    host, port, user, password, use_ssl = _get_smtp_connection()

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = user
    msg["To"] = ", ".join(recipients)
    msg.attach(MIMEText(html, "html"))

    try:
        if use_ssl:
            server = smtplib.SMTP_SSL(host, port, timeout=30)
        else:
            server = smtplib.SMTP(host, port, timeout=30)
            server.starttls()
        server.login(user, password)
        server.sendmail(user, recipients, msg.as_string())
        server.quit()
        logger.info("Email sent successfully to %s", recipients)
    except Exception as e:
        logger.exception("Failed to send email: %s", e)


def on_dag_success(context):
    dag = context.get("dag")
    dr = context.get("dag_run")
    ts = context.get("ts")

    subject = f"DAG Success: {dag.dag_id}"
    html = f"""
    <h3>DAG Succeeded</h3>
    <p><b>DAG:</b> {dag.dag_id}</p>
    <p><b>Run ID:</b> {dr.run_id if dr else ''}</p>
    <p><b>Execution Time:</b> {ts}</p>
    """
    _send_email(subject, html)

def on_dag_failure(context):
    dag = context.get("dag")
    dr = context.get("dag_run")
    ti = context.get("ti")
    ts = context.get("ts")
    exc = context.get("exception")
    log_url = getattr(ti, "log_url", "") if ti else ""

    subject = f"DAG Failure: {dag.dag_id}"
    html = f"""
    <h3>DAG Failed</h3>
    <p><b>DAG:</b> {dag.dag_id}</p>
    <p><b>Run ID:</b> {dr.run_id if dr else ''}</p>
    <p><b>Task:</b> {ti.task_id if ti else ''}</p>
    <p><b>Execution Time:</b> {ts}</p>
    <p><b>Exception:</b> <pre>{exc}</pre></p>
    <p><a href="{log_url}">View Logs</a></p>
    """
    _send_email(subject, html)