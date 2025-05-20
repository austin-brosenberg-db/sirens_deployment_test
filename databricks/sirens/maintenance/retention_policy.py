from dataclasses import dataclass
from enum import IntEnum
from datetime import date
from dateutil.relativedelta import relativedelta
from typing import Optional


@dataclass
class RetentionPolicy:
  """ A retention policy can be defined in sirens.config on three levels:
  [retention_policy] = system level - define nothing else and it applies to all tables
  [retention_policy:bronze] = stage level - more specific than system level and applies to all bronze tables
  [retention_policy:bronze:tablename] - most specific - applies only to that table.
  e.g. The following policy defines that data older than 1 year will be purged for all tables. The date column is `_event_date`.
  [retention_policy]
  duration = 1 years 
  column = _event_date
  """
  columnName: str
  cutoffDate: str
  database: Optional[str] = None
  tableName: Optional[str] = None

  @classmethod
  def parse_policy(cls, policy):
    rp = dict()
    for k, v in policy:
      rp[k] = v
    parts = rp["duration"].strip().split(" ")
    params = { parts[-1].lower(): int(parts[0])}
    cutoffDate = date.today() - relativedelta(**params)
    columnName = rp["column"].strip()
    return RetentionPolicy(columnName=columnName, cutoffDate=cutoffDate)

  @classmethod
  def from_config(cls, globalConfig, db: str, tbl: str):
    if globalConfig.has_section(f"retention_policy:{db}:{tbl}"):
      policy = cls.parse_policy(globalConfig.items(f"retention_policy:{db}:{tbl}"))
    elif globalConfig.has_section(f"retention_policy:{db}"):
      policy = cls.parse_policy(globalConfig.items(f"retention_policy:{db}"))
    elif globalConfig.has_section("retention_policy"):
      policy = cls.parse_policy(globalConfig.items("retention_policy"))
    if policy:
      policy.database = db
      policy.tableName = tbl
    return policy
  
  def _generate_commands(self):
    return f"""DELETE FROM {self.database}.{self.tableName} WHERE {self.columnName} < DATE('{self.cutoffDate}')"""

  def enforce(self, spark):
    try:
      sql = self._generate_commands()
      print(sql)
      spark.sql(sql)
    except Exception as exp:
      print(exp)