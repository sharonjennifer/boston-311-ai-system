"""
Performance Threshold Alerting System

Monitors metrics and triggers alerts when thresholds are exceeded
Zero cost - just logs alerts (can be extended to email/Slack)
"""
import logging
from datetime import datetime
from typing import Dict, List
from collections import deque

logger = logging.getLogger("b311.alerts")


class AlertManager:
    """Monitors metrics and triggers alerts based on thresholds"""
    
    # Alert thresholds
    THRESHOLDS = {
        "error_rate_percent": 10.0,      # Alert if error rate > 10%
        "avg_latency_ms": 5000.0,        # Alert if avg latency > 5 seconds
        "p95_latency_ms": 10000.0,       # Alert if 95th percentile > 10 seconds
        "cache_hit_rate_percent": 20.0,  # Alert if cache hit rate < 20% (too low)
    }
    
    def __init__(self, check_interval_requests: int = 50):
        """
        Initialize alert manager
        
        Args:
            check_interval_requests: Check thresholds every N requests
        """
        self.check_interval = check_interval_requests
        self.request_count = 0
        self.active_alerts = {}
        self.alert_history = deque(maxlen=100)
        logger.info(f"AlertManager initialized (check every {check_interval_requests} requests)")
    
    def check_thresholds(self, metrics: Dict) -> List[Dict]:
        """
        Check if any thresholds are exceeded
        
        Args:
            metrics: Current metrics dictionary
        
        Returns:
            List of triggered alerts
        """
        self.request_count += 1
        
        # Only check every N requests to avoid spam
        if self.request_count % self.check_interval != 0:
            return []
        
        triggered_alerts = []
        
        # Check error rate
        error_rate = metrics.get("error_rate_percent", 0)
        if error_rate > self.THRESHOLDS["error_rate_percent"]:
            alert = self._create_alert(
                "high_error_rate",
                f"Error rate is {error_rate:.1f}% (threshold: {self.THRESHOLDS['error_rate_percent']}%)",
                "critical",
                {
                    "current_value": error_rate,
                    "threshold": self.THRESHOLDS["error_rate_percent"],
                    "total_errors": metrics.get("total_errors", 0)
                }
            )
            triggered_alerts.append(alert)
        
        # Check average latency
        avg_latency = metrics.get("avg_latency_ms", 0)
        if avg_latency > self.THRESHOLDS["avg_latency_ms"]:
            alert = self._create_alert(
                "high_latency",
                f"Average latency is {avg_latency:.0f}ms (threshold: {self.THRESHOLDS['avg_latency_ms']:.0f}ms)",
                "warning",
                {
                    "current_value": avg_latency,
                    "threshold": self.THRESHOLDS["avg_latency_ms"]
                }
            )
            triggered_alerts.append(alert)
        
        # Log all triggered alerts
        for alert in triggered_alerts:
            self._log_alert(alert)
            self.alert_history.append(alert)
        
        return triggered_alerts
    
    def _create_alert(self, alert_type: str, message: str, severity: str, details: Dict) -> Dict:
        """Create alert dictionary"""
        return {
            "type": alert_type,
            "message": message,
            "severity": severity,
            "timestamp": datetime.utcnow().isoformat(),
            "details": details
        }
    
    def _log_alert(self, alert: Dict):
        """Log alert with appropriate severity"""
        severity = alert["severity"]
        message = f"[ALERT] {alert['type'].upper()}: {alert['message']}"
        
        if severity == "critical":
            logger.error(message)
        elif severity == "warning":
            logger.warning(message)
        else:
            logger.info(message)
    
    def get_active_alerts(self) -> List[Dict]:
        """Get currently active alerts"""
        return list(self.active_alerts.values())
    
    def get_alert_history(self) -> List[Dict]:
        """Get recent alert history"""
        return list(self.alert_history)
    
    def acknowledge_alert(self, alert_type: str):
        """Acknowledge and clear an alert"""
        if alert_type in self.active_alerts:
            del self.active_alerts[alert_type]
            logger.info(f"Alert acknowledged: {alert_type}")


# Global alert manager
_alert_manager = None


def get_alert_manager() -> AlertManager:
    """Get global alert manager instance"""
    global _alert_manager
    if _alert_manager is None:
        _alert_manager = AlertManager(check_interval_requests=50)
    return _alert_manager


def check_and_alert(metrics: Dict):
    """
    Convenience function to check thresholds and trigger alerts
    
    Args:
        metrics: Current metrics dictionary
    """
    alert_manager = get_alert_manager()
    return alert_manager.check_thresholds(metrics)