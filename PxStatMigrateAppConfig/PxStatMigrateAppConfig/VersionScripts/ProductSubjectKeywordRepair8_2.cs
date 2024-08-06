using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using API;
using Microsoft.Data.SqlClient;
using PxLanguagePlugin;
using PxLanguagePlugin.en;
using PxLanguagePlugin.ga;

namespace PxStatMigrateAppConfig.VersionScripts
{
    internal static class ProductSubjectKeywordRepair8_2
    {
        internal static void Run(string connectionString)
        {

            PxLanguagePlugin.en.Language english = new();
            PxLanguagePlugin.ga.Language gaeilge = new();
          
            
            List<PrcVersion> allProducts = ReadAllProductLanguages(connectionString);
            List<SbjVersion> allSubjects = ReadAllSubjectLanguages(connectionString);

            var prcCandidateIds = allProducts.GroupBy(x => x.PrcId).Select(y => new { pId = y.Key, total = y.Count() }).Where(z=>z.total>1).ToList().Select(w=>w.pId).ToList<int>();
            var sbjCandidateIds = allSubjects.GroupBy(x => x.SbjId).Select(y => new { sId = y.Key, total = y.Count() }).Where(z => z.total > 1).ToList().Select(w=>w.sId).ToList<int>();

            List<PrcVersion> someProducts=allProducts.Where(x=>prcCandidateIds.Contains(x.PrcId)).ToList();
            List<SbjVersion> someSubjects = allSubjects.Where(x => sbjCandidateIds.Contains(x.SbjId)).ToList();

            List<ProductKeyword> productKeywords = new ();

            foreach (var prd in someProducts)
            {
                List<string> kws = prd.LngIsoCode.Equals("en") ? ExtractSplitSingularEn(english, prd.PrcValue) : ExtractSplitSingularGa(gaeilge, prd.PrcValue);
                foreach (var item in kws)
                {
                    ProductKeyword pw = new ProductKeyword() { KprValue = item, PrcCode = prd.PrcCode };
                    productKeywords.Add(pw);
                }                         
            }

            List<SubjectKeyword> subjectKeywords = new ();

            foreach (var sbj in someSubjects)
            {
                List<string> kws = sbj.LngIsoCode.Equals("en") ? ExtractSplitSingularEn(english, sbj.SbjValue ) : ExtractSplitSingularGa(gaeilge, sbj.SbjValue);
                foreach (var item in kws)
                {
                    SubjectKeyword sw = new SubjectKeyword() { KsjValue  = item, SbjCode  = sbj.SbjCode };
                    subjectKeywords.Add(sw);
                }
            }

            SqlConnection openCon = new SqlConnection(connectionString);
            openCon.Open();
            SqlTransaction tx=openCon.BeginTransaction();
            try
            {
                foreach (var item in productKeywords)
                {
                    if (openCon.State.Equals(ConnectionState.Closed)) openCon.Open();
                    Console.WriteLine("Product " + item.PrcCode + " Keyword update: " + item.KprValue);
                    UpdateProductKeyword(tx,openCon, item);
                }

                foreach (var item in subjectKeywords)
                {
                    if (openCon.State.Equals(ConnectionState.Closed)) openCon.Open();
                    Console.WriteLine("Subject " + item.SbjCode + " Keyword update: " + item.KsjValue);
                    UpdateSubjectKeyword(tx,openCon, item);
                }

                tx.Commit();
            }
            catch(Exception ex)
            {
                tx.Rollback();
                throw ex;
            }
            finally
            {
                if(openCon.State.Equals(ConnectionState.Open)) openCon.Close();
            }


            return;
        }



        internal static List<SbjVersion> ReadAllSubjectLanguages(string connectionString)
        {
            decimal version = 0;
            List<SbjVersion> subjects = new();
            using (SqlConnection openCon = new SqlConnection(connectionString))
            {
                string readLatestCommand = "System_Navigation_Subject_ReadAllLanguages";
                if (openCon.State.Equals(ConnectionState.Closed)) openCon.Open();

                using (SqlCommand queryReadLatest = new SqlCommand(readLatestCommand))
                {
                    
                    queryReadLatest.CommandType = CommandType.StoredProcedure;
                    queryReadLatest.Connection = openCon;

                    SqlDataReader dr = queryReadLatest.ExecuteReader();
                    while (dr.Read())
                    {
                        SbjVersion sbj = new();
                        sbj.SbjId = (int)dr[0];
                        sbj.SbjCode = (int)dr[1];
                        sbj.SbjValue = (string)dr[2];
                        sbj.LngIsoCode = (string)dr[3];
                        subjects.Add(sbj);
                    }

                    dr.Close();
                }
            }

            return subjects;
        }

        internal static void UpdateProductKeyword(SqlTransaction tx, SqlConnection openCon, ProductKeyword kpr)
        {
  
            
                string createConfigCommand = "System_Navigation_Keyword_Product_Create";

                using (SqlCommand queryCreateConfig = new SqlCommand(createConfigCommand))
                {
                    queryCreateConfig.Connection = openCon;
                    queryCreateConfig.CommandType = CommandType.StoredProcedure;
                    queryCreateConfig.Parameters.Add("@KprValue", SqlDbType.NVarChar, 256).Value = kpr.KprValue;
                    queryCreateConfig.Parameters.Add("@PrcCode", SqlDbType.NVarChar, 32).Value = kpr.PrcCode ;
                    queryCreateConfig.Parameters.Add("@KprSingularisedFlag", SqlDbType.Bit).Value = true;
                    queryCreateConfig.Parameters.Add("@KprMandatoryFlag", SqlDbType.Bit).Value = true;
                    queryCreateConfig.Transaction = tx;

                    queryCreateConfig.ExecuteNonQuery();

                    
                }

           
        }

        internal static void UpdateSubjectKeyword(SqlTransaction tx, SqlConnection openCon, SubjectKeyword ksj)
        {
            
            string createConfigCommand = "System_Navigation_Keyword_Subject_Create";

            using (SqlCommand queryCreateConfig = new SqlCommand(createConfigCommand))
            {
                queryCreateConfig.Connection = openCon;
                queryCreateConfig.CommandType = CommandType.StoredProcedure;
                queryCreateConfig.Parameters.Add("@KsbValue", SqlDbType.NVarChar, 256).Value = ksj.KsjValue ;
                queryCreateConfig.Parameters.Add("@SbjCode", SqlDbType.Int).Value = ksj.SbjCode ;
                queryCreateConfig.Parameters.Add("@KsbSingularisedFlag", SqlDbType.Bit).Value = true;
                queryCreateConfig.Parameters.Add("@KsbMandatoryFlag", SqlDbType.Bit).Value = true;
                queryCreateConfig.Transaction = tx;

                queryCreateConfig.ExecuteNonQuery();

               
            }

        }

        internal static List<PrcVersion> ReadAllProductLanguages(string connectionString)
        {
            decimal version = 0;
            List<PrcVersion> products = new();
            using (SqlConnection openCon = new SqlConnection(connectionString))
            {
                string readLatestCommand = "System_Navigation_Product_ReadAllLanguages";
                if (openCon.State.Equals(ConnectionState.Closed)) openCon.Open();
                
                using (SqlCommand queryReadLatest = new SqlCommand(readLatestCommand))
                {
                    queryReadLatest.CommandType = CommandType.StoredProcedure;
                    queryReadLatest.Connection = openCon;

                    SqlDataReader dr = queryReadLatest.ExecuteReader();
                    while (dr.Read())
                    {
                        PrcVersion prc = new();
                        prc.PrcId = (int)dr[0];
                        prc.PrcCode = (string)dr[1];
                        prc.PrcValue = (string)dr[2];
                        prc.LngIsoCode= (string)dr[3];
                        products.Add(prc);
                    }

                    dr.Close();
                }
            }

            return products;
        }

        internal static List<string> ExtractSplitSingularEn(PxLanguagePlugin.en.Language language, string readString)
        {
           

            readString = language.Sanitize(readString);

            //We need to treat spaces and non-breaking spaces the same, so we replace any space with a standard space
            Regex rx = new Regex("[\\s]");
            readString = rx.Replace(readString, " ");

            // convert the sentance to a list of words
            List<string> wordListInput = (readString.Split(' ')).Where(x => x.Length > 0).ToList();

            //create an output list
            List<string> wordList = new List<string>();

            foreach (string word in wordListInput)
            {
                //trim white spaces
                string trimWord = Regex.Replace(word, @"^\s+", "");
                trimWord = Regex.Replace(trimWord, @"\s+$", "");

                if (trimWord.Length > 0)
                {
                   
                    //if the word is not in our list of excluded words
                    if (!language.GetExcludedTerms().Contains(trimWord))
                    {

                            //if the word may be changed from singular to plural
                        if (!language.GetDoNotAmend().Contains(trimWord))
                        {
                            //get the singular version if it's singular
                            string wordRead = language.Singularize(trimWord);
                            if (!wordList.Contains(wordRead))
                                wordList.Add(wordRead);
                        }
                        else
                        {
                            //add the word to the output list, but not if it's in the list already
                            if (!wordList.Contains(trimWord))
                                wordList.Add(trimWord);
                        }
                    }

                }
            }
            return wordList;
        }

        internal static List<string> ExtractSplitSingularGa(PxLanguagePlugin.ga.Language language, string readString)
        {

            readString = language.Sanitize(readString);

            //We need to treat spaces and non-breaking spaces the same, so we replace any space with a standard space
            Regex rx = new Regex("[\\s]");
            readString = rx.Replace(readString, " ");

            // convert the sentance to a list of words
            List<string> wordListInput = (readString.Split(' ')).Where(x => x.Length > 0).ToList();

            //create an output list
            List<string> wordList = new List<string>();

            foreach (string word in wordListInput)
            {
                //trim white spaces
                string trimWord = Regex.Replace(word, @"^\s+", "");
                trimWord = Regex.Replace(trimWord, @"\s+$", "");

                if (trimWord.Length > 0)
                {

                    //if the word is not in our list of excluded words
                    if (!language.GetExcludedTerms().Contains(trimWord))
                    {

                        //if the word may be changed from singular to plural
                        if (!language.GetDoNotAmend().Contains(trimWord))
                        {
                            //get the singular version if it's singular
                            string wordRead = language.Singularize(trimWord);
                            if (!wordList.Contains(wordRead))
                                wordList.Add(wordRead);
                        }
                        else
                        {
                            //add the word to the output list, but not if it's in the list already
                            if (!wordList.Contains(trimWord))
                                wordList.Add(trimWord);
                        }
                    }

                }
            }
            return wordList;
        }
    }

    public class SbjVersion
    {
        public int SbjId { get; set; }
        public int SbjCode { get; set; }
        public string SbjValue { get; set;}
        public string LngIsoCode { get; set; }
    }

    public class PrcVersion
    {
        public int PrcId { get; set; }  
        public string PrcCode { get; set; }
        public string PrcValue { get; set; }
        public string LngIsoCode { get; set; }
    }

    public class ProductKeyword
    {
        public string PrcCode { get; set; }
        public string KprValue { get; set; }

    }

    public class SubjectKeyword
    {
        public int SbjCode { get; set; }
        public string KsjValue { get; set; }
    }
}
