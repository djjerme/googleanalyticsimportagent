using System;
using System.Collections.Generic;
using System.Text;
using System.Configuration;
using System.IO;
using SIO = System.IO;
using System.Data.SqlClient;
using SD = System.Diagnostics;
using GoogleAnalyticsAPI;

namespace GoogleAnalyticsImportAgent
{
    class Program
    {
        static void Main(string[] args)
        {
            // Start log writer (append text to log)
            SIO.FileStream log = new SIO.FileStream(System.AppDomain.CurrentDomain.BaseDirectory + "ImportAgent_Log.txt", SIO.FileMode.Append);
            SD.TextWriterTraceListener logger = new SD.TextWriterTraceListener(log);
            SD.Trace.Listeners.Add(logger);
            SD.Trace.WriteLine("\r\n" + DateTime.Now + " Application execution started");

            VerbalizeAndLog("Executing getAnalyticsData");
            GetAnalyticsData();            
        }

        public static void VerbalizeAndLog(string message)
        {
            Console.WriteLine(String.Format("{0}", message));
            SD.Trace.WriteLine(String.Format("{0} {1}", DateTime.Now, message));
        }

        public static void GetAnalyticsData()
        {
            string path;
            string email;
            string profileId;
            string[] dimensions = new string[] { "ga:eventAction,ga:eventLabel,ga:date,ga:hour,ga:minute" };
            string[] metrics = new string[] { "ga:totalEvents" };
            DateTime start = DateTime.Now.AddDays(-1);
            DateTime end = DateTime.Now.AddDays(-1);

            string getAnalyticsSql = String.Empty;
            string getAnalyticsSqlBody = String.Empty;
            string inputFilesPath = ConfigurationManager.AppSettings["inputFilesPath"];
            string fileName = string.Empty;
            DateTime now = System.DateTime.Now;
            string month = now.Month.ToString();
            string day = now.Day.ToString();

            if (month.Length == 1)
            {
                month = "0" + month;
            }
            if (day.Length == 1)
            {
                day = "0" + day;
            }

            fileName = "GoogleAnalytics_" + now.Year.ToString() + month + day + ".txt";
            inputFilesPath += fileName;

            path = ConfigurationManager.AppSettings["keyPath"];
            email = "279029442075-loisvts11h39eeraf1ippdhv160105lg@developer.gserviceaccount.com";
            profileId = "84265044";

            VerbalizeAndLog("Retrieving Analytics data from Google...");

            GoogleAnalyticsAPI.GoogleAnalyticsAPI ga = new GoogleAnalyticsAPI.GoogleAnalyticsAPI(path, email);

            //GoogleAnalyticsAPI ga = new GoogleAnalyticsAPI(path, email);

            //GoogleAnalyticsAPI.AnalyticDataPoint adp = new GoogleAnalyticsAPI.AnalyticDataPoint();
            GoogleAnalyticsAPI.GoogleAnalyticsAPI.AnalyticDataPoint adp = new GoogleAnalyticsAPI.GoogleAnalyticsAPI.AnalyticDataPoint();
            adp = ga.GetAnalyticsData(profileId, dimensions, metrics, start, end);

            VerbalizeAndLog(String.Format("Retrieved {0} rows from Google.", adp.Rows.Count));

            getAnalyticsSql = "INSERT INTO googleanalyticsimport (StoreId, UserId, itemId, FileName, DateTime) ";
            bool first = true;
            foreach (IList<string> value in adp.Rows)
            {
                if (!first)
                {
                    getAnalyticsSqlBody += " UNION ALL ";
                }
                first = false;
                char[] delim = new char[] { '|' };
                if (!value[1].ToString().Contains("|"))
                {
                    value[1] = value[1].ToString() + "|" + value[1].ToString();
                }

                string[] item = value[1].ToString().Split(delim);

                getAnalyticsSqlBody += "SELECT '384','"
                    + value[0].ToString().Trim() + "','"
                    + item[0].Trim() + "','"
                    + item[1].Trim() + "','"
                    + value[2].ToString() + " " + value[3].ToString() + ":" + value[4].ToString() + ":00'";
            }

            if (string.IsNullOrEmpty(getAnalyticsSqlBody))
            {
                VerbalizeAndLog("No data from web service.");
            }
            else
            {
                getAnalyticsSql += getAnalyticsSqlBody;
                VerbalizeAndLog(String.Format("SQL generated for getAnalyticsData: ", getAnalyticsSql));
                writeFile(getAnalyticsSql, inputFilesPath);
                importData(getAnalyticsSql);
            }

        }

        public static void writeFile(string content, string path)
        {
            VerbalizeAndLog("Writing content of imported data to file: " + path);
            using (StreamWriter outfile = new StreamWriter(path))
            {
                outfile.Write(content);
            }
        }

        public static void importData(string sql)
        {
            VerbalizeAndLog("Importing data into database.");
            SqlConnection connection = new SqlConnection(ConfigurationManager.AppSettings["dbConnection"].ToString());
            try
            {
                SqlCommand cmd = new SqlCommand();
                cmd.CommandTimeout = 600;
                cmd.Connection = connection;
                connection.Open();
                SqlCommand command = new SqlCommand(sql, connection);
                command.CommandTimeout = 1200;
                VerbalizeAndLog("Executing SQL: " + sql.ToString());
                command.ExecuteNonQuery();
                connection.Close();
            }
            catch (Exception ex)
            {
                VerbalizeAndLog("Exception in importData: " + ex.Message);
                return;
            }
        }           
            
    }
}
         
 
