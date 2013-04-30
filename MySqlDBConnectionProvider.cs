using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data.Common;
using System.Configuration;
using System.Data;

namespace LiteMySqlDBProvider.MySqlDataAccessLayer
{
    public sealed class MySqlDBConnectionProvider
    {
        /// <summary>
        /// Esta classe pode ser instanciada mas não pode ser herdada.
        /// </summary>
        public MySqlDBConnectionProvider()
        {
            
        }

        /// <summary>
        /// Retorna um objeto DBConnection fechado utilizando o provider e a string de conexão padrão do web.config. Deve seer aberto/fechado quando necessário.
        /// </summary>
        /// <returns>Objeto DbConnection</returns>
        public DbConnection GetConnection()
        {
                try
                {
                    //Utiliza a chave "provider" para retornar o provider padrão definido no AppSettings
                    DbProviderFactory Dbfactory = MySqlDBConnectionProvider.GetDbFactory();

                    //Cria o objeto da conexão a partir do "_providerKey"
                    DbConnection conn = Dbfactory.CreateConnection();

                    //Utiliza a chave "connectionString" para retornar a connectionString do AppSettings
                    conn.ConnectionString = GetDbConnectionString();
                    
                    return conn;
                }
                catch (DbException)
                {
                    throw new Exception("Uma exceção ocorreu durante a criação da conexão. Por favor verifique as configurações da string de conexão do web.config.");
                }
        }

        /// <summary>
        /// Retorna um objeto DBConnection fechado utilizando o provider padrão e a string de conexão da "_connectionKey". Deve ser aberto/fechado quando necessário.
        /// </summary>
        /// <returns>Objeto DbConnection</returns>
        public DbConnection GetConnection(string _connectionKey)
        {
            try
            {
                //Utiliza a chave "provider" para retornar o provider padrão definido no AppSettings
                DbProviderFactory Dbfactory = MySqlDBConnectionProvider.GetDbFactory();

                //Cria o objeto da conexão a partir da chave "provider"
                DbConnection conn = Dbfactory.CreateConnection();

                //Utiliza a chave "_connectionKey" para retornar a connectionString do AppSettings
                conn.ConnectionString = GetDbConnectionString(_connectionKey);

                return conn;
            }
            catch (DbException)
            {
                throw new Exception("Uma exceção ocorreu durante a criação da conexão. Por favor verifique as configurações da string de conexão do web.config.");
            }
        }

        /// <summary>
        /// Retorna um objeto DBConnection fechado. Deve ser aberto/fechado quando necessário.
        /// </summary>
        /// <param name="_providerKey">Chave específica de um provider do AppSettings</param>
        /// <param name="_connectionKey">Chave específica de uma connectionString do AppSettings</param>
        /// <returns>Objeto DbConnection</returns>
        public DbConnection GetConnection(string _providerKey, string _connectionKey)
        {
            try
            {
                //Utiliza a chave "_providerKey" para retornar um provider do tipo definido no AppSettings 
                DbProviderFactory Dbfactory = MySqlDBConnectionProvider.GetDbFactory(_providerKey);
                
                //Cria o objeto da conexão a partir do "_providerKey"
                DbConnection conn = Dbfactory.CreateConnection();
                
                //Utiliza a chave "_connectionKey" para retornar a connectionString do AppSettings
                conn.ConnectionString = GetDbConnectionString(_connectionKey);

                return conn;
            }
            catch (DbException)
            {
                throw new Exception("Uma exceção ocorreu durante a criação da conexão. Por favor verifique as configurações da string de conexão do web.config.");
            }
        }

        /// <summary>
        /// Retorna a ConnectionString do AppSettings do web.config. 
        /// </summary>
        /// <returns>string</returns>
        private string GetDbConnectionString()
        {
            //string ConnectionString = Convert.ToString(ConfigurationManager.ConnectionStrings["connectionString"]);
            AppSettingsReader appReader = new AppSettingsReader();

            return appReader.GetValue("connectionString", typeof(string)).ToString();
        }

        /// <summary>
        /// Retorna a ConnectionString do AppSettings do web.config. 
        /// </summary>
        /// <param name="_connectionKey">Nome da connectionString</param>
        /// <returns>string</returns>
        private string GetDbConnectionString(string _connectionKey)
        {
            //string ConnectionString = Convert.ToString(ConfigurationManager.ConnectionStrings["connectionString"]);
            AppSettingsReader appReader = new AppSettingsReader();

            return appReader.GetValue(_connectionKey, typeof(string)).ToString();
        }

        /// <summary>
        /// Utiliza a chave do provider padrão do web.config para retorna um objeto DBProviderFactory válido.
        /// </summary>
        /// <returns>Objeto DbProviderFactory</returns>
        internal static DbProviderFactory GetDbFactory()
        {
            try
            {
                //OLD:string ProviderName = ConfigurationManager.AppSettings["Provider"];
                System.Configuration.AppSettingsReader appReader = new System.Configuration.AppSettingsReader();

                string provider = appReader.GetValue("provider", typeof(string)).ToString();

                DbProviderFactory Dbfactory = DbProviderFactories.GetFactory(provider);
                return Dbfactory;
            }
            catch(DbException)
            {
                throw new Exception("Uma exceção ocorreu durante a criação da conexão. Por favor verifique as configurações da string de conexão do web.config.");
            }
        }

        /// <summary>
        /// Utiliza a chave provider do parâmetro "_providerKey" do web.config para retorna um objeto DBProviderFactory válido.
        /// </summary>
        /// <returns>Objeto DbProviderFactory</returns>
        internal static DbProviderFactory GetDbFactory(string _providerKey)
        {
            try
            {
                System.Configuration.AppSettingsReader appReader = new System.Configuration.AppSettingsReader();

                string provider = appReader.GetValue(_providerKey, typeof(string)).ToString();

                DbProviderFactory Dbfactory = DbProviderFactories.GetFactory(provider);
                return Dbfactory;
            }
            catch (DbException)
            {
                throw new Exception("Uma exceção ocorreu durante a criação da conexão. Por favor verifique as configurações da string de conexão do web.config.");
            }
        }
    }
}
