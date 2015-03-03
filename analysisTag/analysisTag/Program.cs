using Mfg.Comm.Db.Sql;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace analysisTag
{
    class Program
    {
        static void Main(string[] args)
        {
            AnalyasisAddTag();
            Console.ReadKey();
        }

        private static void AnalyasisAddTag()
        {
            string sqlConnKey = ConfigurationManager.AppSettings["conName"];
            string mongoConnKey = "mongo_" + sqlConnKey.Trim();
            int oneQuestStar = Convert.ToInt32(ConfigurationManager.AppSettings["oneQuestStart"]);
            int twoQuestStar = Convert.ToInt32(ConfigurationManager.AppSettings["twoQuestStart"]);
            if (oneQuestStar != -1)
            {
                while (true)
                {
                    int tem = oneQuestStar;
                    string resu = UpdateOneQuest(sqlConnKey, mongoConnKey, oneQuestStar, out oneQuestStar);
                    if (string.IsNullOrEmpty(resu))
                    {
                        Console.WriteLine("一次试题ok");
                        break;
                    }
                    else
                    {
                        Console.WriteLine(tem + "~" + oneQuestStar + ":ok");
                    }
                }
            }
            if (twoQuestStar != -1)
            {
                while (true)
                {
                    int tem = twoQuestStar;
                    string resu = UpdateTwoQuest(sqlConnKey, mongoConnKey, twoQuestStar, out twoQuestStar);

                    if (string.IsNullOrEmpty(resu))
                    {
                        Console.WriteLine("二次试题ok");
                        break;
                    }
                    else
                    {
                        Console.WriteLine(tem + "~" + twoQuestStar + ":ok");
                    }
                }
            }
        }

        private static string UpdateTwoQuest(string sqlConnKey, string mongoConnKey, int starId, out int lastId)
        {
            string sql = "select top 1000 f_id from ddt_t_questions where f_id>@id and f_updatetime>'2014-12-13' order by f_id";
            SqlParameter[] sqlpara = new SqlParameter[]{
              new SqlParameter("@id",(object)starId)
            };
            DataTable dt = DBHelper.GetDataTable(sqlConnKey, sql, sqlpara);
            if (dt != null && dt.Rows.Count > 0)
            {
                lastId = Convert.ToInt32(dt.Rows[dt.Rows.Count - 1]["f_id"]);
                foreach (DataRow row in dt.Rows)
                {
                    int id = Convert.ToInt32(row["f_id"]);
                    try
                    {
                        MongoHelper2 monghelp = new MongoHelper2(mongoConnKey, "ddt_t_questions");
                        monghelp.UpdateById(id, "isMaster", 1);
                    }
                    catch (Exception ex)
                    {

                        Console.WriteLine("标记为母题异常：" + id);
                    }
                }
                return "1";

            }
            else
            {
                lastId = -1;
                return null;
            }
        }
        private static string UpdateOneQuest(string sqlConnKey, string mongoConnKey, int starId, out int lastId)
        {

            string sql = "select top 1000 f_id from ddt_t_question where f_id>@id and f_updatetime>'2014-12-13' order by f_id";
            SqlParameter[] sqlpara = new SqlParameter[]{
              new SqlParameter("@id",(object)starId)
            };
            DataTable dt = DBHelper.GetDataTable(sqlConnKey, sql, sqlpara);
            if (dt != null && dt.Rows.Count > 0)
            {
                lastId = Convert.ToInt32(dt.Rows[dt.Rows.Count - 1]["f_id"]);
                foreach (DataRow row in dt.Rows)
                {
                    int id = Convert.ToInt32(row["f_id"]);
                    try
                    {
                        MongoHelper2 monghelp = new MongoHelper2(mongoConnKey, "ddt_t_question");
                        monghelp.UpdateById(id, "isMaster", 1);
                    }
                    catch (Exception ex)
                    {

                        Console.WriteLine("标记为母题异常：" + id);
                    }
                }
                return "1";

            }
            else
            {
                lastId = -1;
                return null;
            }
        }
    }
}
