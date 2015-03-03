using System;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
namespace Mfg.Comm.Db.Sql
{
    public static class DBHelper
    {
        /// <summary>保存数据操作的出错记录
        /// </summary>
        /// <param name="db">数据库id</param>
        /// <param name="cmdType">命令类型（0 sql不带参数  1 sql带参数）</param>
        /// <param name="cmdStr">命令字符串</param>
        /// <param name="errMsg">错误信息内容</param>
        /// <param name="paras">参数列表</param>
        private static void saveProcError(string db, int cmdType, string cmdStr, string errMsg, params SqlParameter[] paras)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.Append("{\"time\":\"");
            stringBuilder.Append(DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"));
            stringBuilder.Append("\",\"db\":\"");
            stringBuilder.Append(db);
            stringBuilder.Append("\",\"cmdType\":");
            stringBuilder.Append(cmdType.ToString());
            stringBuilder.Append(",\"cmdStr\":\"");
            stringBuilder.Append(cmdStr);
            stringBuilder.Append("\",err:\"");
            stringBuilder.Append(errMsg.Replace("\"", "\\\""));
            stringBuilder.Append("\"");
            if (paras != null)
            {
                stringBuilder.Append(",\"para\":[");
                for (int i = 0; i < paras.Length; i += 2)
                {
                    stringBuilder.Append("\"");
                    stringBuilder.Append(paras[i].ParameterName);
                    stringBuilder.Append("\",\"");
                    stringBuilder.Append(paras[i].Value.ToString().Replace("\"", "\\\""));
                    stringBuilder.Append("\",\"");
                    stringBuilder.Append((paras[i].Direction == ParameterDirection.Input) ? "I\"" : "O\"");
                }
                stringBuilder.Append("]");
            }
            stringBuilder.Append("}\r\n");
            if (ConfigurationManager.AppSettings["errorlogpath"] != null)
            {
                File.AppendAllText(ConfigurationManager.AppSettings["errorlogpath"], stringBuilder.ToString(), Encoding.UTF8);
            }
        }
        /// <summary>组装分页查询的SQL语句
        /// </summary>
        /// <param name="fieldList">查询的字段列表，不含“SELECT”</param>
        /// <param name="tableAndCondition">查询的表和条件，不含“FROM”</param>
        /// <param name="orderWay">排序方式，不含“ORDER BY”</param>
        /// <param name="pageSize">每页显示的条数</param>
        /// <param name="pageIndex">需要显示第几页</param>
        /// <param name="neekct">是否需要计算总数</param>
        /// <returns>返回SQL语句</returns>
        private static string getPageSql(string fieldList, string tableAndCondition, string orderWay, int pageSize, int pageIndex, bool neekct)
        {
            int num = (pageIndex - 1) * pageSize;
            if (num < 0)
            {
                num = 0;
            }
            StringBuilder stringBuilder = new StringBuilder(string.Empty);
            if (neekct)
            {
                int num2 = tableAndCondition.IndexOf(" group ", StringComparison.OrdinalIgnoreCase);
                if (num2 > -1)
                {
                    stringBuilder.Append("WITH tb0 AS (SELECT ");
                    stringBuilder.Append(tableAndCondition.Substring(num2 + 9));
                    stringBuilder.Append(" FROM ");
                    stringBuilder.Append(tableAndCondition);
                    stringBuilder.Append(") SELECT COUNT(*) FROM tb0;");
                }
                else
                {
                    stringBuilder.Append("SELECT count(*) FROM ");
                    stringBuilder.Append(tableAndCondition);
                    stringBuilder.Append(";");
                }
            }
            stringBuilder.Append("WITH tb1 AS (SELECT ");
            stringBuilder.Append(fieldList);
            stringBuilder.Append(",ROW_NUMBER() OVER( ORDER BY ");
            stringBuilder.Append(orderWay);
            stringBuilder.Append(") AS __ord");
            stringBuilder.Append(" FROM ");
            stringBuilder.Append(tableAndCondition);
            stringBuilder.Append(" ) SELECT TOP ");
            stringBuilder.Append(pageSize.ToString());
            stringBuilder.Append(" ");
            stringBuilder.Append("*");
            stringBuilder.Append(" FROM tb1 WHERE __ord>");
            stringBuilder.Append(num.ToString());
            stringBuilder.Append(" order by __ord");
            return stringBuilder.ToString();
        }
        /// <summary>生成存储过程输入参数（char）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToCharInP(this string _content, string _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.Char
            };
        }
        /// <summary>生成存储过程输入参数（varchar）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToVarCharInP(this string _content, string _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.VarChar
            };
        }
        /// <summary>生成存储过程输入参数（int）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToIntInP(this string _content, int _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.Int
            };
        }
        /// <summary>生成存储过程输入参数（Datetime）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToDateTimeInP(this string _content, DateTime _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.DateTime
            };
        }
        /// <summary>生成存储过程输入参数（Text）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToTextInP(this string _content, string _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.Text
            };
        }
        /// <summary>生成存储过程输入参数（NText）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToNTextInP(this string _content, string _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.NText
            };
        }
        /// <summary>生成存储过程输入参数（NChar）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToNCharInP(this string _content, string _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.NChar
            };
        }
        /// <summary>生成存储过程输入参数（NVarChar）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToNVarCharInP(this string _content, string _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.NVarChar
            };
        }
        /// <summary>生成存储过程输入参数（Float）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToFloatInP(this string _content, float _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.Float
            };
        }
        /// <summary>生成存储过程输入参数（Image，二进制流）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToImageInP(this string _content, byte[] _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.Image
            };
        }
        /// <summary>生成存储过程输出参数（char）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_size">参数的长度</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToCharOutP(this string _content, int _size)
        {
            return new SqlParameter
            {
                SqlDbType = SqlDbType.Char,
                ParameterName = _content,
                Direction = ParameterDirection.Output,
                Size = _size
            };
        }
        /// <summary>生成存储过程输出参数（varchar）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_size">参数的长度</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToVarCharOutP(this string _content, int _size)
        {
            return new SqlParameter
            {
                SqlDbType = SqlDbType.VarChar,
                ParameterName = _content,
                Direction = ParameterDirection.Output,
                Size = _size
            };
        }
        /// <summary>生成存储过程输出参数（int）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToIntOutP(this string _content)
        {
            return new SqlParameter
            {
                SqlDbType = SqlDbType.Int,
                ParameterName = _content,
                Direction = ParameterDirection.Output
            };
        }
        /// <summary>生成存储过程输出参数（Float）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToFloatOutP(this string _content)
        {
            return new SqlParameter
            {
                SqlDbType = SqlDbType.Float,
                ParameterName = _content,
                Direction = ParameterDirection.Output
            };
        }
        /// <summary>生成存储过程输出参数（DateTime）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToDateTimeOutP(this string _content)
        {
            return new SqlParameter
            {
                SqlDbType = SqlDbType.DateTime,
                ParameterName = _content,
                Direction = ParameterDirection.Output
            };
        }
        /// <summary>生成存储过程输出参数（Text）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToTextOutP(this string _content)
        {
            return new SqlParameter
            {
                SqlDbType = SqlDbType.Text,
                Direction = ParameterDirection.Output
            };
        }
        /// <summary>生成存储过程输出参数（NText）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToNTextOutP(this string _content)
        {
            return new SqlParameter
            {
                ParameterName = _content,
                SqlDbType = SqlDbType.NText,
                Direction = ParameterDirection.Output
            };
        }
        /// <summary>生成存储过程输出参数（NChar）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_size">参数的长度</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToNCharOutP(this string _content, int _size)
        {
            return new SqlParameter
            {
                SqlDbType = SqlDbType.NChar,
                ParameterName = _content,
                Size = _size,
                Direction = ParameterDirection.Output
            };
        }
        /// <summary>生成存储过程输出参数（NVarChar）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_size">参数的长度</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToNVarCharOutP(this string _content, int _size)
        {
            return new SqlParameter
            {
                SqlDbType = SqlDbType.NVarChar,
                ParameterName = _content,
                Direction = ParameterDirection.Output,
                Size = _size
            };
        }
        /// <summary>生成存储过程输出参数（Image,二进制数组）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToImageOutP(this string _content)
        {
            return new SqlParameter
            {
                SqlDbType = SqlDbType.Image,
                Direction = ParameterDirection.Output
            };
        }
        /// <summary>生成存储过程输入输出参数（Char）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">输入的参数值</param>
        /// <param name="_size">参数的长度</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToCharIOP(this string _content, string _value, int _size)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.Char,
                Direction = ParameterDirection.InputOutput,
                Size = _size
            };
        }
        /// <summary>生成存储过程输入输出参数（VarChar）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">输入的参数值</param>
        /// <param name="_size">参数的长度</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToVarCharIOP(this string _content, string _value, int _size)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.VarChar,
                Direction = ParameterDirection.InputOutput,
                Size = _size
            };
        }
        /// <summary>生成存储过程输入输出参数（NChar）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">输入的参数值</param>
        /// <param name="_size">参数的长度</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToNCharIOP(this string _content, string _value, int _size)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.NChar,
                Direction = ParameterDirection.InputOutput,
                Size = _size
            };
        }
        /// <summary>生成存储过程输入输出参数（NVarChar）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">输入的参数值</param>
        /// <param name="_size">参数的长度</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToNVarCharIOP(this string _content, string _value, int _size)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.NVarChar,
                Direction = ParameterDirection.InputOutput,
                Size = _size
            };
        }
        /// <summary>生成存储过程输入输出参数（int）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">输入的参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToIntIOP(this string _content, int _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.Int,
                Direction = ParameterDirection.InputOutput
            };
        }
        /// <summary>生成存储过程输入输出参数（float）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">输入的参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToFloatIOP(this string _content, float _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.Float,
                Direction = ParameterDirection.InputOutput
            };
        }
        /// <summary>生成存储过程输入输出参数（DateTime）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">输入的参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToDateTimeIOP(this string _content, DateTime _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.DateTime,
                Direction = ParameterDirection.InputOutput
            };
        }
        /// <summary>生成存储过程输入输出参数（Text）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">输入的参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToTextIOP(this string _content, string _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.Text,
                Direction = ParameterDirection.InputOutput
            };
        }
        /// <summary>生成存储过程输入输出参数（NText）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">输入的参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToNTextIOP(this string _content, string _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.NText,
                Direction = ParameterDirection.InputOutput
            };
        }
        /// <summary>生成存储过程输入输出参数（Image,二进制数组）
        /// </summary>
        /// <param name="_content">参数名</param>
        /// <param name="_value">输入的参数值</param>
        /// <returns>sqlParameter</returns>
        public static SqlParameter ToImageIOP(this string _content, byte[] _value)
        {
            return new SqlParameter(_content, _value)
            {
                SqlDbType = SqlDbType.Image,
                Direction = ParameterDirection.InputOutput
            };
        }
        /// <summary>执行SQL语句并返回影响的行数
        /// </summary>
        /// <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        /// <param name="sqlStr">要执行的查询SQL语句</param>
        /// <param name="_paras">可选参数数组</param>
        /// <returns>执行SQL语句影响行数，执行失败返回 -1 </returns>
        public static int SendSQL(string conStr, string sqlStr, params SqlParameter[] _paras)
        {
            int result = -1;
            using (SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings[conStr].ConnectionString))
            {
                SqlCommand sqlCommand = new SqlCommand(sqlStr, sqlConnection);
                sqlCommand.CommandType = CommandType.Text;
                if (_paras != null)
                {
                    sqlCommand.Parameters.AddRange(_paras);
                }
                try
                {
                    sqlConnection.Open();
                    try
                    {
                        result = sqlCommand.ExecuteNonQuery();
                    }
                    catch (SqlException ex)
                    {
                        DBHelper.saveProcError(conStr, (_paras == null) ? 0 : 1, sqlStr, ex.Message, _paras);
                    }
                    finally
                    {
                        sqlConnection.Close();
                    }
                }
                catch (SqlException ex)
                {
                    DBHelper.saveProcError(conStr, (_paras == null) ? 0 : 1, sqlStr, ex.Message, _paras);
                }
            }
            return result;
        }
        /// <summary> 获取数据集对象
        /// </summary>
        /// <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        /// <param name="sqlStr">要执行的查询SQL语句</param>
        /// <param name="_paras">可选参数数组</param>
        /// <returns>得到的数据集，发生错误则返回 null</returns>
        public static DataSet GetDataSet(string conStr, string sqlStr, params SqlParameter[] _paras)
        {
            DataSet dataSet = null;
            DataSet result;
            using (SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings[conStr].ConnectionString))
            {
                SqlCommand sqlCommand = new SqlCommand(sqlStr, sqlConnection);
                sqlCommand.CommandType = CommandType.Text;
                sqlCommand.CommandTimeout = 10000;
                if (_paras != null)
                {
                    sqlCommand.Parameters.AddRange(_paras);
                }
                SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(sqlCommand);
                try
                {
                    dataSet = new DataSet();
                    sqlDataAdapter.Fill(dataSet);
                }
                catch (SqlException ex)
                {
                    DBHelper.saveProcError(conStr, (_paras == null) ? 0 : 1, sqlStr, ex.Message, _paras);
                    result = null;
                    return result;
                }
            }
            result = dataSet;
            return result;
        }
        /// <summary>获取数据表对象
        /// </summary>
        /// <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        /// <param name="sqlStr">要执行的查询SQL语句</param>
        /// <param name="_paras">可选参数数组</param>
        /// <returns>得到的数据表，发生错误则返回 null</returns>
        public static DataTable GetDataTable(string conStr, string sqlStr, params SqlParameter[] _paras)
        {
            DataSet dataSet = DBHelper.GetDataSet(conStr, sqlStr, _paras);
            DataTable result;
            if (dataSet != null)
            {
                result = dataSet.Tables[0];
            }
            else
            {
                result = null;
            }
            return result;
        }
        /// <summary>获取数据表对象
        /// </summary>
        /// <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        /// <param name="sqlStr">要执行的查询SQL语句</param>
        /// <param name="myDa">需要返回的中间桥接器</param>
        /// <param name="_paras">可选参数数组</param>
        /// <returns>得到的数据表，发生错误则返回 null</returns>
        public static DataTable GetDataTableByAdpater(string conStr, string sqlStr, ref SqlDataAdapter myDa, params SqlParameter[] _paras)
        {
            DataTable dataTable = null;
            SqlConnection connection = new SqlConnection(ConfigurationManager.ConnectionStrings[conStr].ConnectionString);
            SqlCommand sqlCommand = new SqlCommand(sqlStr, connection);
            sqlCommand.CommandType = CommandType.Text;
            if (_paras != null)
            {
                sqlCommand.Parameters.AddRange(_paras);
            }
            myDa = new SqlDataAdapter(sqlCommand);
            DataTable result;
            try
            {
                dataTable = new DataTable();
                myDa.Fill(dataTable);
            }
            catch (SqlException ex)
            {
                DBHelper.saveProcError(conStr, (_paras == null) ? 0 : 1, sqlStr, ex.Message, _paras);
                result = null;
                return result;
            }
            result = dataTable;
            return result;
        }
        /// <summary>获取DataReader对象
        /// </summary>
        /// <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        /// <param name="sqlStr">要执行的查询SQL语句</param>
        /// <param name="_paras">可选参数数组</param>
        /// <returns>得到的DataReader，发生错误则返回 null</returns>
        public static SqlDataReader GetDataReader(string conStr, string sqlStr, params SqlParameter[] _paras)
        {
            SqlDataReader sqlDataReader = null;
            SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings[conStr].ConnectionString);
            SqlCommand sqlCommand = new SqlCommand(sqlStr, sqlConnection);
            sqlCommand.CommandType = CommandType.Text;
            if (_paras != null)
            {
                sqlCommand.Parameters.AddRange(_paras);
            }
            SqlDataReader result;
            try
            {
                sqlConnection.Open();
                try
                {
                    sqlDataReader = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
                }
                catch (SqlException ex)
                {
                    DBHelper.saveProcError(conStr, (_paras == null) ? 0 : 1, sqlStr, ex.Message, _paras);
                    sqlConnection.Close();
                    result = null;
                    return result;
                }
            }
            catch (SqlException ex)
            {
                DBHelper.saveProcError(conStr, (_paras == null) ? 0 : 1, sqlStr, ex.Message, _paras);
            }
            result = sqlDataReader;
            return result;
        }
        /// <summary>获取单个字段值
        /// </summary>
        /// <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        /// <param name="sqlStr">要执行的查询SQL语句</param>
        /// <param name="_paras">可选参数数组</param>
        /// <returns>返回查询的字段</returns>
        public static object GetField(string conStr, string sqlStr, params SqlParameter[] _paras)
        {
            object result = null;
            using (SqlDataReader dataReader = DBHelper.GetDataReader(conStr, sqlStr, _paras))
            {
                if (dataReader != null && dataReader.HasRows)
                {
                    dataReader.Read();
                    result = dataReader[0];
                }
                if (dataReader != null)
                {
                    dataReader.Close();
                }
            }
            return result;
        }
        /// <summary>执行存储过程得到DataReader对象
        /// </summary>
        /// <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        /// <param name="stpName">存储过程名称</param>
        /// <param name="stpPara">可选参数数组</param>
        /// <returns>DataReader对象,注：DataReader的NextResult可用的情况下，分别为输出参数列表和返回值</returns>
        public static SqlDataReader ExecStoreProcR(string conStr, string stpName, params SqlParameter[] stpPara)
        {
            SqlDataReader result = null;
            SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings[conStr].ConnectionString);
            SqlCommand sqlCommand = new SqlCommand(stpName, sqlConnection);
            sqlCommand.CommandType = CommandType.StoredProcedure;
            if (stpPara != null)
            {
                sqlCommand.Parameters.AddRange(stpPara);
            }
            try
            {
                sqlConnection.Open();
                try
                {
                    result = sqlCommand.ExecuteReader(CommandBehavior.CloseConnection);
                }
                catch (SqlException ex)
                {
                    DBHelper.saveProcError(conStr, (stpPara == null) ? 0 : 1, stpName, ex.Message, stpPara);
                    sqlConnection.Close();
                }
            }
            catch (SqlException ex)
            {
                DBHelper.saveProcError(conStr, (stpPara == null) ? 0 : 1, stpName, ex.Message, stpPara);
            }
            return result;
        }
        /// <summary>执行存储过程返回传出值参数列表，返回值在stpPara参数中获取
        /// </summary>
        /// <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        /// <param name="stpName">存储过程名称</param>
        /// <param name="stpPara">可选参数数组</param>
        public static void ExecStoreProcP(string conStr, string stpName, params SqlParameter[] stpPara)
        {
            using (SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings[conStr].ConnectionString))
            {
                SqlCommand sqlCommand = new SqlCommand(stpName, sqlConnection);
                sqlCommand.CommandType = CommandType.StoredProcedure;
                if (stpPara != null)
                {
                    sqlCommand.Parameters.AddRange(stpPara);
                }
                try
                {
                    sqlConnection.Open();
                    try
                    {
                        sqlCommand.ExecuteNonQuery();
                    }
                    catch (SqlException ex)
                    {
                        DBHelper.saveProcError(conStr, (stpPara == null) ? 0 : 1, stpName, ex.Message, stpPara);
                    }
                    finally
                    {
                        sqlConnection.Close();
                    }
                }
                catch (SqlException ex)
                {
                    DBHelper.saveProcError(conStr, (stpPara == null) ? 0 : 1, stpName, ex.Message, stpPara);
                }
            }
        }
        /// <summary>执行存储过程得到DataSet对象，并返回传出值参数列表
        /// </summary>
        /// <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        /// <param name="stpName">存储过程名称</param>
        /// <param name="stpPara">存储过程参数列表</param>
        /// <returns>DataSet对象</returns>
        public static DataSet ExecStoreProcS(string conStr, string stpName, params SqlParameter[] stpPara)
        {
            DataSet dataSet = null;
            using (SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings[conStr].ConnectionString))
            {
                SqlCommand sqlCommand = new SqlCommand(stpName, sqlConnection);
                sqlCommand.CommandType = CommandType.StoredProcedure;
                if (stpPara != null)
                {
                    sqlCommand.Parameters.AddRange(stpPara);
                }
                SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(sqlCommand);
                dataSet = new DataSet();
                try
                {
                    sqlDataAdapter.Fill(dataSet);
                }
                catch (SqlException ex)
                {
                    DBHelper.saveProcError(conStr, (stpPara == null) ? 0 : 1, stpName, ex.Message, stpPara);
                }
            }
            return dataSet;
        }
        /// <summary>执行存储过程得到DataTable对象
        /// </summary>
        /// <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        /// <param name="stpName">存储过程名称</param>
        /// <param name="stpPara">存储过程参数列表</param>
        /// <returns>DataTable对象</returns>
        public static DataTable ExecStoreProcT(string conStr, string stpName, params SqlParameter[] _paras)
        {
            DataTable result = null;
            using (DataSet dataSet = DBHelper.ExecStoreProcS(conStr, stpName, _paras))
            {
                if (dataSet != null)
                {
                    result = dataSet.Tables[0];
                }
            }
            return result;
        }
        /// <summary>获取分页的Datareader对象
        ///  </summary>
        ///  <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        ///  <param name="fieldList">查询的字段列表，不含“SELECT”</param>
        ///  <param name="tableAndCondition">查询的表和条件，不含“FROM”</param>
        ///  <param name="orderWay">排序方式，不含“ORDER BY”</param>
        ///  <param name="pageSize">每页显示的条数</param>
        ///  <param name="pageIndex">需要显示第几页</param>
        ///  <param name="rsCount">返回得到总记录条数，如果出错返回-1 </param>
        ///  <param name="_paras">参数列表</param>
        ///  <returns>DbDataReader对象，如果出错返回null</returns>
        public static SqlDataReader GetPageReader(string conStr, string fieldList, string tableAndCondition, string orderWay, int pageSize, int pageIndex, out int rsCount, params SqlParameter[] _paras)
        {
            SqlDataReader dataReader = DBHelper.GetDataReader(conStr, DBHelper.getPageSql(fieldList, tableAndCondition, orderWay, pageSize, pageIndex, true), _paras);
            SqlDataReader result;
            if (dataReader != null)
            {
                dataReader.Read();
                rsCount = int.Parse(dataReader[0].ToString());
                dataReader.NextResult();
                result = dataReader;
            }
            else
            {
                rsCount = -1;
                result = null;
            }
            return result;
        }
        /// <summary>获取分页数据的Datareader对象
        ///  </summary>
        ///  <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        ///  <param name="fieldList">查询的字段列表，不含“SELECT”</param>
        ///  <param name="tableAndCondition">查询的表和条件，不含“FROM”</param>
        ///  <param name="orderWay">排序方式，不含“ORDER BY”</param>
        ///  <param name="pageSize">每页显示的条数</param>
        ///  <param name="pageIndex">需要显示第几页</param>
        ///  <param name="_paras">参数列表</param>
        ///  <returns>DbDataReader对象，如果出错返回null</returns>
        public static SqlDataReader GetPageReader(string conStr, string fieldList, string tableAndCondition, string orderWay, int pageSize, int pageIndex, params SqlParameter[] _paras)
        {
            return DBHelper.GetDataReader(conStr, DBHelper.getPageSql(fieldList, tableAndCondition, orderWay, pageSize, pageIndex, false), _paras);
        }
        /// <summary>获取分页数据的Table对象
        ///  </summary>
        ///  <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        ///  <param name="fieldList">查询的字段列表，不含“SELECT”</param>
        ///  <param name="tableAndCondition">查询的表和条件，不含“FROM”</param>
        ///  <param name="orderWay">排序方式，不含“ORDER BY”</param>
        ///  <param name="pageSize">每页显示的条数</param>
        ///  <param name="pageIndex">需要显示第几页</param>
        ///  <param name="_paras">参数列表</param>
        ///  <param name="rsCount">返回得到总记录条数,如果出错返回-1</param>
        ///  <returns>Table对象，如果出错返回null</returns>
        public static DataTable GetPageTable(string conStr, string fieldList, string tableAndCondition, string orderWay, int pageSize, int pageIndex, out int rsCount, params SqlParameter[] _paras)
        {
            DataSet dataSet = DBHelper.GetDataSet(conStr, DBHelper.getPageSql(fieldList, tableAndCondition, orderWay, pageSize, pageIndex, true), _paras);
            DataTable result;
            if (dataSet != null)
            {
                rsCount = int.Parse(dataSet.Tables[0].Rows[0][0].ToString());
                result = dataSet.Tables[1];
            }
            else
            {
                rsCount = -1;
                result = null;
            }
            return result;
        }
        /// <summary>获取分页数据的Table对象
        /// </summary>
        /// <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        /// <param name="fieldList">查询的字段列表，不含“SELECT”</param>
        /// <param name="tableAndCondition">查询的表和条件，不含“FROM”</param>
        /// <param name="orderWay">排序方式，不含“ORDER BY”</param>
        /// <param name="pageSize">每页显示的条数</param>
        /// <param name="pageIndex">需要显示第几页</param>
        /// <param name="_paras">参数列表</param>
        /// <returns>Table对象</returns>
        public static DataTable GetPageTable(string conStr, string fieldList, string tableAndCondition, string orderWay, int pageSize, int pageIndex, params SqlParameter[] _paras)
        {
            return DBHelper.GetDataTable(conStr, DBHelper.getPageSql(fieldList, tableAndCondition, orderWay, pageSize, pageIndex, false), _paras);
        }
        /// <summary> 批量插入数据表数据
        /// </summary>
        /// <param name="conStr">要访问的数据库编码</param>
        /// <param name="sTable">存有数据的内存数据表</param>
        /// <param name="dTableName">数据库目标数据表</param>
        /// <param name="withTran">是否启用事务</param>
        /// <param name="maps">传入添加字段的数组  索引单数为源数据的列名   索引为双数的时候为对应单数的目标列目标列名</param>
        /// <returns>bool值，表示是否成功</returns>
        public static bool BlockInsertTable(string conStr, DataTable sTable, string dTableName, bool withTran, params string[] maps)
        {
            bool result = false;
            using (SqlBulkCopy sqlBulkCopy = new SqlBulkCopy(ConfigurationManager.ConnectionStrings[conStr].ConnectionString, withTran ? SqlBulkCopyOptions.UseInternalTransaction : SqlBulkCopyOptions.Default))
            {
                sqlBulkCopy.DestinationTableName = dTableName;
                sqlBulkCopy.BatchSize = 1000;
                if (maps != null)
                {
                    for (int i = 0; i < maps.Length; i += 2)
                    {
                        sqlBulkCopy.ColumnMappings.Add(maps[i], maps[i + 1]);
                    }
                }
                try
                {
                    sqlBulkCopy.WriteToServer(sTable);
                    result = true;
                }
                catch (SqlException ex)
                {
                    DBHelper.saveProcError(conStr, 2, "BulkCopy", ex.Message, null);
                }
                finally
                {
                    sqlBulkCopy.Close();
                }
            }
            return result;
        }
        /// <summary>保存数据集中的修改到数据库
        /// </summary>
        /// <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        /// <param name="sqlStr">填充数据集使用的select sql语句（语句中的字段要跟数据集中每个表的字段完全对应，且无需条件）</param>
        /// <param name="Ds">需要更新的数据集</param>
        /// <returns></returns>
        public static bool SaveDataSet(string conStr, string sqlStr, DataSet Ds)
        {
            bool result;
            using (SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings[conStr].ConnectionString))
            {
                SqlCommand selectCommand = new SqlCommand(sqlStr, sqlConnection);
                SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(selectCommand);
                SqlCommandBuilder sqlCommandBuilder = new SqlCommandBuilder(sqlDataAdapter);
                try
                {
                    sqlDataAdapter.Update(Ds);
                }
                catch (SqlException ex)
                {
                    DBHelper.saveProcError(conStr, 3, sqlStr, ex.Message, new SqlParameter[0]);
                    result = false;
                    return result;
                }
                result = true;
            }
            return result;
        }
        /// <summary>保存TataTable中的数据修改到数据库
        /// </summary>
        /// <param name="conStr">在配置文件中配置的数据库连接字符串的键值</param>
        /// <param name="sqlStr">获取DataTable中数据的sql语句（sql语句中的字段必须跟DataTable中字段完全相同，且无需条件）</param>
        /// <param name="Dt">要保存数据的TataTable</param>
        /// <returns></returns>
        public static bool SaveDataTable(string conStr, string sqlStr, DataTable Dt)
        {
            bool result;
            using (SqlConnection sqlConnection = new SqlConnection(ConfigurationManager.ConnectionStrings[conStr].ConnectionString))
            {
                SqlCommand selectCommand = new SqlCommand(sqlStr, sqlConnection);
                SqlDataAdapter sqlDataAdapter = new SqlDataAdapter(selectCommand);
                SqlCommandBuilder sqlCommandBuilder = new SqlCommandBuilder(sqlDataAdapter);
                try
                {
                    sqlDataAdapter.Update(Dt);
                }
                catch (SqlException ex)
                {
                    DBHelper.saveProcError(conStr, 3, sqlStr, ex.Message, new SqlParameter[0]);
                    result = false;
                    return result;
                }
                result = true;
            }
            return result;
        }
    }
}
