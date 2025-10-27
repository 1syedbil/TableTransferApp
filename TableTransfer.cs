using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace TableTransferApp
{
    internal class TableTransfer
    {
        public string SourceConnectionString { get; set; } = string.Empty;
        public string SourceDatabase { get; set; } = string.Empty;
        public string SourceTable { get; set; } = string.Empty;

        public string DestConnectionString { get; set; } = string.Empty;
        public string DestDatabase { get; set; } = string.Empty;
        public string DestTable { get; set; } = string.Empty;

        public int ExecuteTransfer()
        {
            if (HasAnyEmpty(SourceConnectionString, SourceDatabase, SourceTable,
                            DestConnectionString, DestDatabase, DestTable))
            {
                throw new ArgumentException("All fields are required.");
            }

            string srcSchema, srcTableName;
            SplitSchemaTable(SourceTable, out srcSchema, out srcTableName);

            string dstSchema, dstTableName;
            SplitSchemaTable(DestTable, out dstSchema, out dstTableName);

            using (var srcConn = new SqlConnection(SourceConnectionString))
            using (var dstConn = new SqlConnection(DestConnectionString))
            {
                OpenAndEnsureDatabaseExists(srcConn, SourceDatabase,
                    "Source database '" + SourceDatabase + "' does not exist. Please re-enter the source database.");

                OpenAndEnsureDatabaseExists(dstConn, DestDatabase,
                    "Destination database '" + DestDatabase + "' does not exist. Please re-enter the destination database.");

                srcConn.ChangeDatabase(SourceDatabase);
                dstConn.ChangeDatabase(DestDatabase);

                if (!TableExists(srcConn, srcSchema, srcTableName))
                {
                    throw new ArgumentException("Source table '" + srcSchema + "." + srcTableName +
                                                "' does not exist in database '" + SourceDatabase + "'. Please correct the source table name.");
                }

                List<ColumnDef> srcColumns = ReadTableSchemaViaInformationSchema(srcConn, srcSchema, srcTableName);
                if (srcColumns.Count == 0)
                {
                    throw new ArgumentException("Unable to read schema for source table '" + srcSchema + "." + srcTableName + "'.");
                }

                bool destExists = TableExists(dstConn, dstSchema, dstTableName);
                if (destExists)
                {
                    List<ColumnDef> dstColumns = ReadTableSchemaViaInformationSchema(dstConn, dstSchema, dstTableName);
                    if (!SchemasMatch(srcColumns, dstColumns))
                    {
                        throw new ArgumentException("Destination table '" + dstSchema + "." + dstTableName +
                                                    "' exists but does not match the source schema. Please correct the destination table name.");
                    }
                }
                else
                {
                    string createSql = BuildCreateTableSql(dstSchema, dstTableName, srcColumns);
                    using (var cmd = new SqlCommand(createSql, dstConn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }

                using (var tx = dstConn.BeginTransaction(IsolationLevel.ReadCommitted))
                {
                    try
                    {
                        int rows = BulkCopy(srcConn, srcSchema, srcTableName, dstConn, tx, dstSchema, dstTableName);
                        tx.Commit();
                        return rows;
                    }
                    catch
                    {
                        tx.Rollback();
                        throw;
                    }
                }
            }
        }

        private static bool HasAnyEmpty(params string[] parts)
        {
            for (int i = 0; i < parts.Length; i++)
            {
                if (string.IsNullOrWhiteSpace(parts[i])) return true;
            }
            return false;
        }

        private static void SplitSchemaTable(string input, out string schema, out string table)
        {
            string trimmed = input.Trim().Trim('[', ']');
            int dot = trimmed.IndexOf('.');
            if (dot >= 0)
            {
                schema = trimmed.Substring(0, dot).Trim().Trim('[', ']');
                table = trimmed.Substring(dot + 1).Trim().Trim('[', ']');
            }
            else
            {
                schema = "dbo";
                table = trimmed;
            }
        }

        private static void OpenAndEnsureDatabaseExists(SqlConnection conn, string database, string onMissingMessage)
        {
            conn.Open();

            using (var existsCmd = new SqlCommand("SELECT 1 FROM sys.databases WHERE name = @db", conn))
            {
                existsCmd.Parameters.AddWithValue("@db", database);
                object exists = existsCmd.ExecuteScalar();
                if (exists == null)
                {
                    throw new ArgumentException(onMissingMessage);
                }
            }
        }

        private static bool TableExists(SqlConnection conn, string schema, string table)
        {
            using (var cmd = new SqlCommand(
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = @s AND TABLE_NAME = @t;", conn))
            {
                cmd.Parameters.AddWithValue("@s", schema);
                cmd.Parameters.AddWithValue("@t", table);
                object o = cmd.ExecuteScalar();
                return o != null;
            }
        }

        private static List<ColumnDef> ReadTableSchemaViaInformationSchema(SqlConnection conn, string schema, string table)
        {
            var list = new List<ColumnDef>();

            using (var cmd = new SqlCommand(@"
                SELECT 
                    COLUMN_NAME,
                    DATA_TYPE,
                    CHARACTER_MAXIMUM_LENGTH,
                    NUMERIC_PRECISION,
                    NUMERIC_SCALE,
                    IS_NULLABLE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = @s AND TABLE_NAME = @t
                ORDER BY ORDINAL_POSITION;", conn))
            {
                cmd.Parameters.AddWithValue("@s", schema);
                cmd.Parameters.AddWithValue("@t", table);

                using (var rdr = cmd.ExecuteReader())
                {
                    while (rdr.Read())
                    {
                        var col = new ColumnDef();
                        col.Name = rdr.GetString(0);
                        col.DataType = rdr.GetString(1);
                        col.CharMaxLength = rdr.IsDBNull(2) ? (int?)null : Convert.ToInt32(rdr[2]);
                        col.NumericPrecision = rdr.IsDBNull(3) ? (byte?)null : Convert.ToByte(rdr[3]);
                        col.NumericScale = rdr.IsDBNull(4) ? (int?)null : Convert.ToInt32(rdr[4]);
                        string nullable = rdr.GetString(5);
                        col.IsNullable = string.Equals(nullable, "YES", StringComparison.OrdinalIgnoreCase);

                        list.Add(col);
                    }
                }
            }

            return list;
        }

        private class ColumnDef
        {
            public string Name = "";
            public string DataType = "";
            public int? CharMaxLength;
            public byte? NumericPrecision;
            public int? NumericScale;
            public bool IsNullable;
        }

        private static bool SchemasMatch(IReadOnlyList<ColumnDef> src, IReadOnlyList<ColumnDef> dst)
        {
            if (src.Count != dst.Count) return false;

            for (int i = 0; i < src.Count; i++)
            {
                ColumnDef a = src[i];
                ColumnDef b = dst[i];

                if (!a.Name.Equals(b.Name, StringComparison.OrdinalIgnoreCase)) return false;
                if (!a.DataType.Equals(b.DataType, StringComparison.OrdinalIgnoreCase)) return false;

                if (!NullableEquals(a.CharMaxLength, b.CharMaxLength)) return false;
                if (!NullableEquals(a.NumericPrecision, b.NumericPrecision)) return false;
                if (!NullableEquals(a.NumericScale, b.NumericScale)) return false;
                if (a.IsNullable != b.IsNullable) return false;
            }
            return true;
        }

        private static bool NullableEquals<T>(Nullable<T> x, Nullable<T> y) where T : struct
        {
            if (x.HasValue != y.HasValue) return false;
            if (!x.HasValue) return true;
            return EqualityComparer<T>.Default.Equals(x.Value, y.Value);
        }

        private static string BuildCreateTableSql(string schema, string table, IReadOnlyList<ColumnDef> cols)
        {
            var sb = new StringBuilder();
            sb.Append("CREATE TABLE ").Append(Bracket(schema)).Append('.').Append(Bracket(table)).Append(" (");

            for (int i = 0; i < cols.Count; i++)
            {
                ColumnDef c = cols[i];
                sb.Append(Bracket(c.Name)).Append(' ').Append(BuildTypeSpec(c))
                  .Append(c.IsNullable ? " NULL" : " NOT NULL");
                if (i < cols.Count - 1) sb.Append(", ");
            }

            sb.Append(");");
            return sb.ToString();
        }

        private static string BuildTypeSpec(ColumnDef c)
        {
            string dt = c.DataType.ToLowerInvariant();
            switch (dt)
            {
                case "char":
                case "nchar":
                case "varchar":
                case "nvarchar":
                case "binary":
                case "varbinary":
                    string len;
                    if (c.CharMaxLength.HasValue)
                        len = (c.CharMaxLength.Value == -1) ? "max" : c.CharMaxLength.Value.ToString();
                    else
                        len = "max";
                    return c.DataType + "(" + len + ")";

                case "decimal":
                case "numeric":
                    byte p = c.NumericPrecision.HasValue ? c.NumericPrecision.Value : (byte)18;
                    int s = c.NumericScale.HasValue ? c.NumericScale.Value : 0;
                    return c.DataType + "(" + p + "," + s + ")";

                default:
                    return c.DataType; // e.g., int, bigint, bit, datetime, date, float, real, text, etc.
            }
        }

        private static string Bracket(string ident)
        {
            return "[" + ident + "]";
        }

        private static int BulkCopy(SqlConnection srcConn, string srcSchema, string srcTable,
                                    SqlConnection dstConn, SqlTransaction tx,
                                    string dstSchema, string dstTable)
        {
            // Count source rows to report how many were copied
            int sourceCount = 0;
            using (var countCmd = new SqlCommand("SELECT COUNT(*) FROM " + Bracket(srcSchema) + "." + Bracket(srcTable) + ";", srcConn))
            {
                object o = countCmd.ExecuteScalar();
                sourceCount = Convert.ToInt32(o);
            }

            string selectSql = "SELECT * FROM " + Bracket(srcSchema) + "." + Bracket(srcTable) + ";";
            using (var selectCmd = new SqlCommand(selectSql, srcConn))
            using (var reader = selectCmd.ExecuteReader(CommandBehavior.SequentialAccess))
            using (var bulk = new SqlBulkCopy(dstConn, SqlBulkCopyOptions.CheckConstraints | SqlBulkCopyOptions.FireTriggers, tx))
            {
                bulk.DestinationTableName = Bracket(dstSchema) + "." + Bracket(dstTable);
                bulk.BulkCopyTimeout = 0;

                int fieldCount = reader.FieldCount;
                for (int i = 0; i < fieldCount; i++)
                {
                    string colName = reader.GetName(i);
                    bulk.ColumnMappings.Add(colName, colName);
                }

                bulk.WriteToServer(reader);
            }

            return sourceCount;
        }
    }
}
