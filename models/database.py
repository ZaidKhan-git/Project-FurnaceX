"""
Database Module - SQLite storage for leads.
"""

import sqlite3
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from contextlib import contextmanager

import sys
sys.path.append(str(__file__).rsplit('\\', 2)[0])
from config.settings import DB_PATH

logger = logging.getLogger(__name__)


class Database:
    """SQLite database manager for leads."""
    
    SCHEMA = """
    CREATE TABLE IF NOT EXISTS leads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        company_name TEXT NOT NULL,
        signal_type TEXT NOT NULL,
        source TEXT NOT NULL,
        source_url TEXT,
        product_match TEXT,
        sector TEXT,
        score REAL DEFAULT 0,
        confidence REAL DEFAULT 0,
        territory TEXT,
        sales_officer TEXT,
        keywords_matched TEXT,
        raw_data TEXT,
        discovered_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        status TEXT DEFAULT 'NEW',
        notes TEXT,
        UNIQUE(company_name, signal_type, source_url)
    );
    
    CREATE INDEX IF NOT EXISTS idx_leads_score ON leads(score DESC);
    CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
    CREATE INDEX IF NOT EXISTS idx_leads_discovered ON leads(discovered_at DESC);
    CREATE INDEX IF NOT EXISTS idx_leads_product ON leads(product_match);
    
    CREATE TABLE IF NOT EXISTS scrape_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT NOT NULL,
        started_at DATETIME NOT NULL,
        completed_at DATETIME,
        records_found INTEGER DEFAULT 0,
        records_new INTEGER DEFAULT 0,
        status TEXT DEFAULT 'RUNNING',
        error_message TEXT
    );
    """
    
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()
        
    def _init_db(self):
        """Initialize database schema."""
        with self.get_connection() as conn:
            conn.executescript(self.SCHEMA)
            logger.info(f"Database initialized at {self.db_path}")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def insert_lead(self, lead: Dict[str, Any]) -> Optional[int]:
        """Insert a new lead, returns ID or None if duplicate."""
        with self.get_connection() as conn:
            try:
                cursor = conn.execute("""
                    INSERT INTO leads (
                        company_name, signal_type, source, source_url,
                        product_match, sector, score, confidence, territory, sales_officer, 
                        keywords_matched, raw_data, discovered_at, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    lead.get("company_name", "Unknown"),
                    lead.get("signal_type", "Unknown"),
                    lead.get("source", "Unknown"),
                    lead.get("source_url"),
                    lead.get("product_match"),
                    lead.get("sector"),
                    lead.get("score", 0),
                    lead.get("confidence", 0),
                    lead.get("territory"),
                    lead.get("sales_officer"),
                    json.dumps(lead.get("keywords_matched", {})),
                    json.dumps(lead.get("raw_data", {})),
                    lead.get("discovered_at", datetime.now().isoformat()),
                    lead.get("status", "NEW"),
                ))
                return cursor.lastrowid
            except sqlite3.IntegrityError:
                # Duplicate entry
                return None
    
    def insert_leads_batch(self, leads: List[Dict[str, Any]]) -> Dict[str, int]:
        """Batch insert leads, returns counts."""
        inserted = 0
        duplicates = 0
        
        for lead in leads:
            result = self.insert_lead(lead)
            if result:
                inserted += 1
            else:
                duplicates += 1
        
        logger.info(f"Batch insert: {inserted} new, {duplicates} duplicates")
        return {"inserted": inserted, "duplicates": duplicates}
    
    def get_leads(
        self,
        status: Optional[str] = None,
        product: Optional[str] = None,
        min_score: Optional[float] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Query leads with filters."""
        query = "SELECT * FROM leads WHERE 1=1"
        params = []
        
        if status:
            query += " AND status = ?"
            params.append(status)
        if product:
            query += " AND product_match LIKE ?"
            params.append(f"%{product}%")
        if min_score is not None:
            query += " AND score >= ?"
            params.append(min_score)
            
        query += " ORDER BY score DESC, discovered_at DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        
        with self.get_connection() as conn:
            rows = conn.execute(query, params).fetchall()
            return [dict(row) for row in rows]
    
    def get_lead_by_id(self, lead_id: int) -> Optional[Dict[str, Any]]:
        """Get a specific lead by ID."""
        with self.get_connection() as conn:
            row = conn.execute(
                "SELECT * FROM leads WHERE id = ?", (lead_id,)
            ).fetchone()
            return dict(row) if row else None
    
    def update_lead_status(self, lead_id: int, status: str, notes: str = None):
        """Update lead status."""
        with self.get_connection() as conn:
            conn.execute(
                "UPDATE leads SET status = ?, notes = ? WHERE id = ?",
                (status, notes, lead_id)
            )
    
    def get_stats(self) -> Dict[str, Any]:
        """Get dashboard statistics."""
        with self.get_connection() as conn:
            stats = {}
            
            # Total leads
            stats["total_leads"] = conn.execute(
                "SELECT COUNT(*) FROM leads"
            ).fetchone()[0]
            
            # By status
            rows = conn.execute(
                "SELECT status, COUNT(*) as count FROM leads GROUP BY status"
            ).fetchall()
            stats["by_status"] = {row["status"]: row["count"] for row in rows}
            
            # By product
            rows = conn.execute(
                "SELECT product_match, COUNT(*) as count FROM leads "
                "WHERE product_match IS NOT NULL GROUP BY product_match"
            ).fetchall()
            stats["by_product"] = {row["product_match"]: row["count"] for row in rows}
            
            # High-score leads (>70)
            stats["high_score_leads"] = conn.execute(
                "SELECT COUNT(*) FROM leads WHERE score >= 70"
            ).fetchone()[0]
            
            # Recent (last 24h)
            stats["recent_leads"] = conn.execute(
                "SELECT COUNT(*) FROM leads WHERE discovered_at >= datetime('now', '-1 day')"
            ).fetchone()[0]
            
            return stats
    
    def log_scrape(self, source: str) -> int:
        """Start a scrape log entry."""
        with self.get_connection() as conn:
            cursor = conn.execute(
                "INSERT INTO scrape_logs (source, started_at) VALUES (?, ?)",
                (source, datetime.now().isoformat())
            )
            return cursor.lastrowid
    
    def complete_scrape_log(
        self, log_id: int, records_found: int, records_new: int, 
        status: str = "COMPLETED", error: str = None
    ):
        """Complete a scrape log entry."""
        with self.get_connection() as conn:
            conn.execute("""
                UPDATE scrape_logs SET 
                    completed_at = ?,
                    records_found = ?,
                    records_new = ?,
                    status = ?,
                    error_message = ?
                WHERE id = ?
            """, (
                datetime.now().isoformat(),
                records_found, records_new, status, error, log_id
            ))


# Singleton instance
_db_instance = None

def get_db() -> Database:
    """Get or create database singleton."""
    global _db_instance
    if _db_instance is None:
        _db_instance = Database()
    return _db_instance
