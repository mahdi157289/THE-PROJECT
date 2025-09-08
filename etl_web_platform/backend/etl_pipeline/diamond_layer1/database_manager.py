# src/diamond_layer/database_manager.py
import logging
from sqlalchemy import text, inspect
from typing import List, Dict

class DiamondDatabaseManager:
    
    def __init__(self, engine, logger=None):
        self.engine = engine
        self.logger = logger or logging.getLogger(__name__)
        self._configure_logger() 
        
    def _table_exists(self, table_name: str, schema: str = 'public') -> bool:
        """Check if table exists"""
        return inspect(self.engine).has_table(table_name, schema=schema)
    
    def _get_table_columns(self, table_name: str, schema: str = 'public') -> Dict[str, str]:
        """Get current column definitions"""
        inspector = inspect(self.engine)
        return {col['name']: str(col['type']) for col in inspector.get_columns(table_name, schema=schema)}
    
    def execute_ddl(self, ddl_statements: List[str], force_recreate: bool = False):
        """Execute DDL with safeguards"""
        with self.engine.connect() as conn:
            for stmt in ddl_statements:
                try:
                    if 'CREATE TABLE' in stmt.upper():
                        table_name = self._extract_table_name(stmt)
                        if self._table_exists(table_name):
                            if force_recreate:
                                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
                                self.logger.info(f"Dropped existing table {table_name}")
                            else:
                                self._alter_existing_table(conn, table_name, stmt)
                                continue
                    
                    conn.execute(text(stmt))
                    conn.commit()
                    self.logger.info(f"Executed DDL: {stmt[:100]}...")
                except Exception as e:
                    conn.rollback()
                    self.logger.error(f"DDL execution failed: {str(e)}")
                    raise

    # -------------------- New: Predictions table management --------------------
    def create_diamond_predictions_table(self, force_recreate: bool = False):
        """Create a normalized table for model predictions"""
        predictions_ddl = [
            """
            CREATE TABLE IF NOT EXISTS diamond_predictions (
                id SERIAL PRIMARY KEY,
                test_name VARCHAR(255) NOT NULL,
                model_type VARCHAR(50) NOT NULL,
                execution_date DATE NOT NULL,
                valeur TEXT,
                pred_index INTEGER NOT NULL,
                prediction DOUBLE PRECISION NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            # Helpful composite index for fast retrieval in BI
            """
            CREATE INDEX IF NOT EXISTS idx_diamond_predictions_lookup
            ON diamond_predictions (execution_date, model_type, test_name, valeur);
            """
        ]
        self.execute_ddl(predictions_ddl, force_recreate=force_recreate)
    
    def _alter_existing_table(self, conn, table_name: str, create_stmt: str):
        """Handle ALTER TABLE logic"""
        current_columns = self._get_table_columns(table_name)
        new_columns = self._parse_columns_from_ddl(create_stmt)
        
        for col_name, col_type in new_columns.items():
            if col_name not in current_columns:
                alter_stmt = f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}"
                conn.execute(text(alter_stmt))
                self.logger.info(f"Added column {col_name} to {table_name}")
            elif current_columns[col_name] != col_type:
                self.logger.warning(
                    f"Type mismatch for {col_name}: "
                    f"existing {current_columns[col_name]} vs new {col_type}"
                )
    
    def _extract_table_name(self, ddl: str) -> str:
        """Parse table name from CREATE statement"""
        return ddl.split()[2].split('(')[0].strip()
    
    def _parse_columns_from_ddl(self, ddl: str) -> Dict[str, str]:
        """Extract column definitions from DDL"""
        cols_part = ddl.split('(')[1].rsplit(')', 1)[0]
        columns = {}
        for line in cols_part.split(','):
            line = line.strip()
            if line and not line.upper().startswith(('CONSTRAINT', 'PRIMARY KEY')):
                col_parts = line.split()
                columns[col_parts[0]] = ' '.join(col_parts[1:])
        return columns
    def _configure_logger(self):
        """Ensure logger has phase field"""
        if not hasattr(self.logger, 'phase'):
            self.logger.phase = 'DATABASE'
        if not self.logger.handlers:
            # Add basic console handler if none exists
            ch = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            ch.setFormatter(formatter)
            self.logger.addHandler(ch)