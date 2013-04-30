using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data.Common;
using System.Data;
using System.Collections;

namespace LiteMySqlDBProvider.MySqlDataAccessLayer
{
    public sealed class MySqlDBProvider
    {
        //Variáveis Globais da Classe
        #region {...}

        private static MySqlDBConnectionProvider MySqlDBConnectionProvider;
        private static DbConnection dbConnection;
        private static DbProviderFactory factory;
        private static DbCommand command;
        
        #endregion

        /// <summary>
        /// Returns a DbCommand object.
        /// </summary>
        /// <returns>Returns a DbCommand object.</returns>
        public static DbCommand GetCommand()
        {
            DbCommand comm = MySqlDBConnectionProvider.GetDbFactory().CreateCommand();
            return comm;
        }

        /* 
        * 
        * SQLClient, OleDb, Odbc & Oracle Database Provider
        * 
        */

        #region { Instance }

        //Query Instance

        /// <summary>
        /// Executa uma query sem parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>Int32 indicando a quantidade de linhas afetadas (INSERT, UPDATE ou DELETE).</returns>
        public static int Instance(string _inQuery, out string _errorMessage)
        {
            _errorMessage = String.Empty;
            int recordsAffected = 0;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return recordsAffected = -1;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection  = dbConnection;

                dbConnection.Open();

                recordsAffected = command.ExecuteNonQuery();

                dbConnection.Close();
            }
            catch(DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return recordsAffected;
        }

        /// <summary>
        /// Executa uma query sem parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>Int32 indicando a quantidade de linhas afetadas (INSERT, UPDATE ou DELETE).</returns>
        public static int Instance(string _connectionKey, string _inQuery, out string _errorMessage)
        {
            _errorMessage = String.Empty;
            int recordsAffected = 0;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return recordsAffected = -1;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                dbConnection.Open();

                recordsAffected = command.ExecuteNonQuery();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return recordsAffected;
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>Int32 indicando a quantidade de linhas afetadas (INSERT, UPDATE ou DELETE).</returns>
        public static int Instance(string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            DbParameter parameter = null;
            int recordsAffected = 0;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return recordsAffected = -1;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                parameter = factory.CreateParameter();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }      

                dbConnection.Open();

                recordsAffected = command.ExecuteNonQuery();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return recordsAffected;

        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>Int32 indicando a quantidade de linhas afetadas (INSERT, UPDATE ou DELETE).</returns>
        public static int Instance(string _connectionKey, string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            DbParameter parameter = null;
            int recordsAffected = 0;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return recordsAffected = -1;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                parameter = factory.CreateParameter();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                recordsAffected = command.ExecuteNonQuery();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return recordsAffected;

        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Hashtable com o parâmetro.</param>
        /// <returns>Int32 indicando a quantidade de linhas afetadas (INSERT, UPDATE ou DELETE).</returns>
        public static int Instance(string _inQuery, out string _errorMessage, Hashtable _params)
        {
            DbParameter parameter = null;
    
            int recordsAffected = 0;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return recordsAffected = -1;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandType = CommandType.Text;

                parameter = factory.CreateParameter();

                foreach (DictionaryEntry de in _params)
                {
                    parameter.ParameterName = de.Key.ToString();
                    parameter.Value = de.Value;

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                recordsAffected = command.ExecuteNonQuery();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return recordsAffected;
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Hashtable com o parâmetro.</param>
        /// <returns>Int32 indicando a quantidade de linhas afetadas (INSERT, UPDATE ou DELETE).</returns>
        public static int Instance(string _connectionKey, string _inQuery, out string _errorMessage, Hashtable _params)
        {
            DbParameter parameter = null;

            int recordsAffected = 0;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return recordsAffected = -1;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandType = CommandType.Text;

                parameter = factory.CreateParameter();

                foreach (DictionaryEntry de in _params)
                {
                    parameter.ParameterName = de.Key.ToString();
                    parameter.Value = de.Value;

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                recordsAffected = command.ExecuteNonQuery();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return recordsAffected;
        }

        //Procedure InstanceProcedure

        /// <summary>
        /// Executa uma procedure sem parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>Int32 indicando a quantidade de linhas afetadas (INSERT, UPDATE ou DELETE).</returns>
        public static int InstanceProcedure(string _inProcedure, out string _errorMessage)
        {
            _errorMessage = String.Empty;
            int recordsAffected = 0;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return recordsAffected = -1;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                dbConnection.Open();

                recordsAffected = command.ExecuteNonQuery();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return recordsAffected;
        }

        /// <summary>
        /// Executa uma procedure sem parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>Int32 indicando a quantidade de linhas afetadas (INSERT, UPDATE ou DELETE).</returns>
        public static int InstanceProcedure(string _connectionKey, string _inProcedure, out string _errorMessage)
        {
            _errorMessage = String.Empty;
            int recordsAffected = 0;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return recordsAffected = -1;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                dbConnection.Open();

                recordsAffected = command.ExecuteNonQuery();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return recordsAffected;
        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>Int32 indicando a quantidade de linhas afetadas (INSERT, UPDATE ou DELETE).</returns>
        public static int InstanceProcedure(string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DbParameter parameter = null;
            int recordsAffected = 0;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return recordsAffected = -1;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                parameter = factory.CreateParameter();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                recordsAffected = command.ExecuteNonQuery();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return recordsAffected;

        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>Int32 indicando a quantidade de linhas afetadas (INSERT, UPDATE ou DELETE).</returns>
        public static int InstanceProcedure(string _connectionKey, string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DbParameter parameter = null;
            int recordsAffected = 0;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return recordsAffected = -1;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                parameter = factory.CreateParameter();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                recordsAffected = command.ExecuteNonQuery();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return recordsAffected;

        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>Int32 indicando a quantidade de linhas afetadas (INSERT, UPDATE ou DELETE).</returns>
        public static int InstanceProcedure(string _providerKey, string _connectionKey, string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DbParameter parameter = null;
            int recordsAffected = 0;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return recordsAffected = -1;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_providerKey, _connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory(_providerKey);
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                parameter = factory.CreateParameter();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                recordsAffected = command.ExecuteNonQuery();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return recordsAffected;

        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Hashtable com o parâmetro.</param>
        /// <returns>Int32 indicando a quantidade de linhas afetadas (INSERT, UPDATE ou DELETE).</returns>
        public static int InstanceProcedure(string _inProcedure, out string _errorMessage, Hashtable _params)
        {
            DbParameter parameter = null;

            int recordsAffected = 0;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return recordsAffected = -1;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandType = CommandType.StoredProcedure;

                parameter = factory.CreateParameter();

                foreach (DictionaryEntry de in _params)
                {
                    parameter.ParameterName = de.Key.ToString();
                    parameter.Value = de.Value;

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                recordsAffected = command.ExecuteNonQuery();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return recordsAffected;
        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Hashtable com o parâmetro.</param>
        /// <returns>Int32 indicando a quantidade de linhas afetadas (INSERT, UPDATE ou DELETE).</returns>
        public static int InstanceProcedure(string _connectionKey, string _inProcedure, out string _errorMessage, Hashtable _params)
        {
            DbParameter parameter = null;

            int recordsAffected = 0;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return recordsAffected = -1;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandType = CommandType.StoredProcedure;

                parameter = factory.CreateParameter();

                foreach (DictionaryEntry de in _params)
                {
                    parameter.ParameterName = de.Key.ToString();
                    parameter.Value = de.Value;

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                recordsAffected = command.ExecuteNonQuery();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return recordsAffected;
        }

        #endregion

        #region { InstanceDataSet }

        //Query InstanceDataSet

        /// <summary>
        /// Executa uma query sem parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataSet com as linhas da Query.</returns>
        public static DataSet InstanceDataSet(string _inQuery, out string _errorMessage)
        {
            DataSet dataSet = new DataSet();
            DbDataAdapter dataAdapter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection  = dbConnection;

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
            }

            return (dataSet);
        }

        /// <summary>
        /// Executa uma query sem parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataSet com as linhas da Query.</returns>
        public static DataSet InstanceDataSet(string _connectionKey, string _inQuery, out string _errorMessage)
        {
            DataSet dataSet = new DataSet();
            DbDataAdapter dataAdapter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
            }

            return (dataSet);
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataSet com as linhas da Query.</returns>
        public static DataSet InstanceDataSet(string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            DataSet dataSet = new DataSet();
            DbParameter parameter = null;
            DbDataAdapter dataAdapter = null;
            
            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();

                command     = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();
                parameter   = factory.CreateParameter();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection  = dbConnection;

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataSet);
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataSet com as linhas da Query.</returns>
        public static DataSet InstanceDataSet(string _connectionKey, string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            DataSet dataSet = new DataSet();
            DbParameter parameter = null;
            DbDataAdapter dataAdapter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();

                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();
                parameter = factory.CreateParameter();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataSet);
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Hashtable com o parâmetro.</param>
        /// <returns>System.Data.DataSet com as linhas da Query.</returns>
        public static DataSet InstanceDataSet(string _inQuery, out string _errorMessage, Hashtable _params)
        {
            DataSet dataSet = new DataSet();
            DbParameter parameter     = null;
            DbDataAdapter dataAdapter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();

                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();
                parameter = factory.CreateParameter();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                foreach (DictionaryEntry de in _params)
                {
                    parameter.ParameterName = de.Key.ToString();
                    parameter.Value = de.Value;

                    command.Parameters.Add(parameter);
                }

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }
            
            return (dataSet);
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Hashtable com o parâmetro.</param>
        /// <returns>System.Data.DataSet com as linhas da Query.</returns>
        public static DataSet InstanceDataSet(string _connectionKey, string _inQuery, out string _errorMessage, Hashtable _params)
        {
            DataSet dataSet = new DataSet();
            DbParameter parameter = null;
            DbDataAdapter dataAdapter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();

                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();
                parameter = factory.CreateParameter();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                foreach (DictionaryEntry de in _params)
                {
                    parameter.ParameterName = de.Key.ToString();
                    parameter.Value = de.Value;

                    command.Parameters.Add(parameter);
                }

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataSet);
        }

        //Procedure InstanceProcedureDataSet

        /// <summary>
        /// Executa uma procedure sem parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataSet com as linhas da StoredProcedure.</returns>
        public static DataSet InstanceProcedureDataSet(string _inProcedure, out string _errorMessage)
        {
            DataSet dataSet = new DataSet();
            DbDataAdapter dataAdapter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataSet);
        }

        /// <summary>
        /// Executa uma procedure sem parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataSet com as linhas da StoredProcedure.</returns>
        public static DataSet InstanceProcedureDataSet(string _connectionKey, string _inProcedure, out string _errorMessage)
        {
            DataSet dataSet = new DataSet();
            DbDataAdapter dataAdapter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataSet);
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataSet com as linhas da StoredProcedure.</returns>
        public static DataSet InstanceProcedureDataSet(string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DataSet dataSet = new DataSet();
            DbParameter parameter     = null;
            DbDataAdapter dataAdapter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();

                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();
                parameter = factory.CreateParameter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataSet);
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataSet com as linhas da StoredProcedure.</returns>
        public static DataSet InstanceProcedureDataSet(string _connectionKey, string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DataSet dataSet = new DataSet();
            DbParameter parameter = null;
            DbDataAdapter dataAdapter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();

                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();
                parameter = factory.CreateParameter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataSet);
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_hashTable">HashTable com os parâmetros de saída.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataSet com as linhas da StoredProcedure.</returns>
        public static DataSet InstanceProcedureDataSet(string _inProcedure, out string _errorMessage, out Hashtable _hashTable , params DbParameter[] _params)
        {
            _hashTable = new Hashtable();
            DataSet dataSet = new DataSet();
            DbParameter parameter = null;
            DbDataAdapter dataAdapter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();

                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();
                parameter   = factory.CreateParameter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection  = dbConnection;

                //Adiciona os parâmetros de entrada
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);

                }

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);

                //Recupera os valores de saída e insere numa HashTable
                for (int i = 0; i < _params.Length; i++)
                {
                    _hashTable.Add(_params[i].ParameterName, _params[i].Value); //Valores de saída
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataSet);
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_hashTable">HashTable com os parâmetros de saída.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataSet com as linhas da StoredProcedure.</returns>
        public static DataSet InstanceProcedureDataSet(string _connectionKey, string _inProcedure, out string _errorMessage, out Hashtable _hashTable, params DbParameter[] _params)
        {
            _hashTable = new Hashtable();
            DataSet dataSet = new DataSet();
            DbParameter parameter = null;
            DbDataAdapter dataAdapter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();

                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();
                parameter = factory.CreateParameter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                //Adiciona os parâmetros de entrada
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);

                }

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);

                //Recupera os valores de saída e insere numa HashTable
                for (int i = 0; i < _params.Length; i++)
                {
                    _hashTable.Add(_params[i].ParameterName, _params[i].Value); //Valores de saída
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataSet);
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Hashtable com o parâmetro.</param>
        /// <returns>System.Data.DataSet com as linhas da StoredProcedure.</returns>
        public static DataSet InstanceProcedureDataSet(string _inProcedure, out string _errorMessage, Hashtable _params)
        {
            DataSet dataSet = new DataSet();
            DbParameter parameter     = null;
            DbDataAdapter dataAdapter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();

                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();
                parameter = factory.CreateParameter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                foreach (DictionaryEntry de in _params)
                {
                    parameter.ParameterName = de.Key.ToString();
                    parameter.Value = de.Value;

                    command.Parameters.Add(parameter);
                }

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataSet);
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um DataSet, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Hashtable com o parâmetro.</param>
        /// <returns>System.Data.DataSet com as linhas da StoredProcedure.</returns>
        public static DataSet InstanceProcedureDataSet(string _connectionKey, string _inProcedure, out string _errorMessage, Hashtable _params)
        {
            DataSet dataSet = new DataSet();
            DbParameter parameter = null;
            DbDataAdapter dataAdapter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataSet;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();

                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();
                parameter = factory.CreateParameter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                foreach (DictionaryEntry de in _params)
                {
                    parameter.ParameterName = de.Key.ToString();
                    parameter.Value = de.Value;

                    command.Parameters.Add(parameter);
                }

                dataAdapter.SelectCommand = command;
                dataAdapter.Fill(dataSet);
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataSet);
        }

        #endregion

        #region { InstanceDataTable }

        //Query InstanceDataTable

        /// <summary>
        /// Executa uma query sem parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceDataTable(string _inQuery, out string _errorMessage)
        {            
            DataTable dataTable = new DataTable();
            DbDataAdapter dataAdapter = null;
            DbDataReader dataReader   = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection  = dbConnection;

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }

        /// <summary>
        /// Executa uma query sem parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceDataTable(string _connectionKey, string _inQuery, out string _errorMessage)
        {
            DataTable dataTable = new DataTable();
            DbDataAdapter dataAdapter = null;
            DbDataReader dataReader = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }

        /// <summary>
        /// Executa uma query sem parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_providerKey">Chave do Provider do AppSettings do web.config.</param>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceDataTable(string _providerKey, string _connectionKey, string _inQuery, out string _errorMessage)
        {
            DataTable dataTable = new DataTable();
            DbDataReader dataReader = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_providerKey, _connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory(_providerKey);
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }

        /// <summary>
        /// Executa uma query sem parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceDataTable(string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            DataTable dataTable = new DataTable();
            DbDataAdapter dataAdapter = null;
            DbDataReader dataReader   = null;
            DbParameter parameter     = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                command.Connection.Open();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }

        /// <summary>
        /// Executa uma query sem parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceDataTable(string _connectionKey, string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            DataTable dataTable = new DataTable();
            DbDataAdapter dataAdapter = null;
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                command.Connection.Open();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }

        /// <summary>
        /// Executa uma query sem parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_providerKey">Chave do Provider do AppSettings do web.config.</param>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceDataTable(string _providerKey, string _connectionKey, string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            DataTable dataTable = new DataTable();
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_providerKey, _connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory(_providerKey);
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                command.Connection.Open();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }

        /// <summary>
        /// Executa uma query sem parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceDataTable(string _inQuery, out string _errorMessage, Hashtable _params)
        {
            DataTable dataTable = new DataTable();
            DbDataAdapter dataAdapter = null;
            DbDataReader dataReader   = null;
            DbParameter parameter     = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                foreach (DictionaryEntry de in _params)
                {
                    parameter.ParameterName = de.Key.ToString();
                    parameter.Value = de.Value;

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }

        /// <summary>
        /// Executa uma query sem parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceDataTable(string _connectionKey, string _inQuery, out string _errorMessage, Hashtable _params)
        {
            DataTable dataTable = new DataTable();
            DbDataAdapter dataAdapter = null;
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                foreach (DictionaryEntry de in _params)
                {
                    parameter.ParameterName = de.Key.ToString();
                    parameter.Value = de.Value;

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }

        //Procedure InstanceDataTable

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceProcedureDataTable(string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DataTable dataTable = new DataTable();
            DbDataAdapter dataAdapter = null;
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceProcedureDataTable(string _connectionKey, string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DataTable dataTable = new DataTable();
            DbDataAdapter dataAdapter = null;
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }
  
        /// <summary>
        /// Executa uma procedure com parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_providerKey">Chave do Provider do AppSettings do web.config.</param>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceProcedureDataTable(string _providerKey, string _connectionKey, string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DataTable dataTable = new DataTable();
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_providerKey,_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory(_providerKey);
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceProcedureDataTable(string _inProcedure, out string _errorMessage, Hashtable _params)
        {
            DataTable dataTable = new DataTable();
            DbDataAdapter dataAdapter = null;
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                foreach (DictionaryEntry de in _params)
                {
                    parameter.ParameterName = de.Key.ToString();
                    parameter.Value = de.Value;

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceProcedureDataTable(string _connectionKey, string _inProcedure, out string _errorMessage, Hashtable _params)
        {
            DataTable dataTable = new DataTable();
            DbDataAdapter dataAdapter = null;
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                foreach (DictionaryEntry de in _params)
                {
                    parameter.ParameterName = de.Key.ToString();
                    parameter.Value = de.Value;

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando uma DataTable, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_providerKey">Chave do Provider do AppSettings do web.config.</param>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Data.DataTable com as linhas da Query.</returns>
        public static DataTable InstanceProcedureDataTable(string _providerKey, string _connectionKey, string _inProcedure, out string _errorMessage, Hashtable _params)
        {
            DataTable dataTable = new DataTable();
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataTable;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                foreach (DictionaryEntry de in _params)
                {
                    parameter.ParameterName = de.Key.ToString();
                    parameter.Value = de.Value;

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (dataTable);
        }

        //Procedure InstanceProcedureDataTable

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando uma List, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Collections.Generic.List(DataRow) com as linhas da Procedure.</returns>
        public static List<DataRow> InstanceProcedureDataTableOutList(string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DataTable dataTable = new DataTable();
            DbDataAdapter dataAdapter = null;
            DbDataReader dataReader = null;
            DbParameter parameter = null;
            List<DataRow> list = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return list;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                    list = new List<DataRow>(dataTable.Select());
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (list);
        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando uma List, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Collections.Generic.List(DataRow) com as linhas da Procedure.</returns>
        public static List<DataRow> InstanceProcedureDataTableOutList(string _connectionKey, string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DataTable dataTable = new DataTable();
            DbDataAdapter dataAdapter = null;
            DbDataReader dataReader = null;
            DbParameter parameter = null;
            List<DataRow> list = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return list;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();
                dataAdapter = factory.CreateDataAdapter();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                    list = new List<DataRow>(dataTable.Select());
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataAdapter != null)
                {
                    dataAdapter.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (list);
        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando uma List, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_providerKey">Chave do Provider do AppSettings do web.config.</param>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Procedure de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Collections.Generic.List(DataRow) com as linhas da Procedure.</returns>
        public static List<DataRow> InstanceProcedureDataTableOutList(string _providerKey, string _connectionKey, string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DataTable dataTable = new DataTable();
            DbDataReader dataReader = null;
            DbParameter parameter = null;
            List<DataRow> list = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return list;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_providerKey, _connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory(_providerKey);
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (dataReader.HasRows)
                {
                    dataTable.Load(dataReader);
                    list = new List<DataRow>(dataTable.Select());
                }
                else
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dataReader != null)
                {
                    dataReader.Dispose();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return (list);
        }

        #endregion

        #region { InstanceDataReader }
        //IMPORTANTE:O objeto DataReader deve ser fechado explicitamente pelo programador depois de utilizado, seja na camada de apresentação ou na camada de negócio.

        //Query InstanceDataReader

        /// <summary>
        /// Executa uma query sem parâmetros, retornando uma DataReader, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataReader com as linhas da Query.</returns>
        public static DbDataReader InstanceDataReader(string _inQuery, out string _errorMessage)
        {
            DbDataReader dataReader = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataReader;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection  = dbConnection;
                
                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (!dataReader.HasRows)
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    command.Connection.Close();

                    return null;
                }

            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
            }

            return (dataReader);
        }

        /// <summary>
        /// Executa uma query sem parâmetros, retornando uma DataReader, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataReader com as linhas da Query.</returns>
        public static DbDataReader InstanceDataReader(string _connectionKey, string _inQuery, out string _errorMessage)
        {
            DbDataReader dataReader = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataReader;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (!dataReader.HasRows)
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    command.Connection.Close();

                    return null;
                }

            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
            }

            return (dataReader);
        }

        /// <summary>
        /// Executa uma query sem parâmetros, retornando uma DataReader, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_providerKey">Chave do Provider do AppSettings do web.config.</param>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataReader com as linhas da Query.</returns>
        public static DbDataReader InstanceDataReader(string _providerKey, string _connectionKey, string _inQuery, out string _errorMessage)
        {
            DbDataReader dataReader = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataReader;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_providerKey, _connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory(_providerKey);
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);

                if (!dataReader.HasRows)
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    command.Connection.Close();

                    return null;
                }

            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
            }

            return (dataReader);
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando uma DataReader, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataReader com as linhas da Query.</returns>
        public static DbDataReader InstanceDataReader(string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataReader;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                command.Connection.Open();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);
                if (!dataReader.HasRows)
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    command.Connection.Close();

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
            }

            return (dataReader);
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando uma DataReader, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataReader com as linhas da Query.</returns>
        public static DbDataReader InstanceDataReader(string _connectionKey, string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataReader;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                command.Connection.Open();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);
                if (!dataReader.HasRows)
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    command.Connection.Close();

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
            }

            return (dataReader);
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando uma DataReader, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_providerKey">Chave do Provider do AppSettings do web.config.</param>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataReader com as linhas da Query.</returns>
        public static DbDataReader InstanceDataReader(string _providerKey, string _connectionKey, string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataReader;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_providerKey, _connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory(_providerKey);
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                command.Connection.Open();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);
                if (!dataReader.HasRows)
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    command.Connection.Close();

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
            }

            return (dataReader);
        }

        //Procedure InstanceProcedureDataReader

        /// <summary>
        /// Executa uma procedure sem parâmetros, retornando uma DataReader, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataReader com as linhas da StoredProcedure.</returns>
        public static DbDataReader InstanceProcedureDataReader(string _inProcedure, out string _errorMessage)
        {
            DbDataReader dataReader = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataReader;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);
                if (!dataReader.HasRows)
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";
                    
                    command.Connection.Close();

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
            }

            return (dataReader);
        }

        /// <summary>
        /// Executa uma procedure sem parâmetros, retornando uma DataReader, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataReader com as linhas da StoredProcedure.</returns>
        public static DbDataReader InstanceProcedureDataReader(string _connectionKey, string _inProcedure, out string _errorMessage)
        {
            DbDataReader dataReader = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataReader;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);
                if (!dataReader.HasRows)
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    command.Connection.Close();

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
            }

            return (dataReader);
        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando uma DataReader, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataReader com as linhas da StoredProcedure.</returns>
        public static DbDataReader InstanceProcedureDataReader(string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataReader;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);
                if (!dataReader.HasRows)
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    command.Connection.Close();

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
            }

            return (dataReader);
        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando uma DataReader, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_providerKey">Chave do Provider do AppSettings do web.config.</param>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataReader com as linhas da StoredProcedure.</returns>
        public static DbDataReader InstanceProcedureDataReader(string _providerKey, string _connectionKey, string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataReader;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_providerKey, _connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory(_providerKey);
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);
                if (!dataReader.HasRows)
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    command.Connection.Close();

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
            }

            return (dataReader);
        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando uma DataReader, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Data.DataReader com as linhas da StoredProcedure.</returns>
        public static DbDataReader InstanceProcedureDataReader(string _connectionKey, string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            DbDataReader dataReader = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return dataReader;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                command.Connection.Open();

                dataReader = command.ExecuteReader(CommandBehavior.CloseConnection);
                if (!dataReader.HasRows)
                {
                    _errorMessage = "Nenhum registro encontrado.  <BR>";

                    command.Connection.Close();

                    return null;
                }
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
            }

            return (dataReader);
        }

        #endregion

        #region { InstanceQueryOutData }

        //Query InstanceQueryOutData

        /// <summary>
        /// Executa uma query com parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>Hashtable.</returns>
        public static Hashtable InstanceQueryOutData(string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            Hashtable ht = new Hashtable();
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return null;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                parameter = factory.CreateParameter();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                command.ExecuteNonQuery();

                dbConnection.Close();

                //Recupera os valores de saída e insere numa HashTable
                for (int i = 0; i < _params.Length; i++)
                {
                    ht.Add(_params[i].ParameterName, _params[i].Value);
                }

                return ht;
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return ht;

        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>Hashtable.</returns>
        public static Hashtable InstanceQueryOutData(string _connectionKey, string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            Hashtable ht = new Hashtable();
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return null;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                parameter = factory.CreateParameter();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                command.ExecuteNonQuery();

                dbConnection.Close();

                //Recupera os valores de saída e insere numa HashTable
                for (int i = 0; i < _params.Length; i++)
                {
                    ht.Add(_params[i].ParameterName, _params[i].Value);
                }

                return ht;
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return ht;

        }

        //Procedure InstanceProcedureOutData

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>Hashtable.</returns>
        public static Hashtable InstanceProcedureOutData(string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            Hashtable ht = new Hashtable();
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return null;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                parameter = factory.CreateParameter();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                command.ExecuteNonQuery();

                dbConnection.Close();

                //Recupera os valores de saída e insere numa HashTable
                for (int i = 0; i < _params.Length; i++)
                {
                    ht.Add(_params[i].ParameterName, _params[i].Value);
                }

                return ht;
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return ht;

        }

        /// <summary>
        /// Executa uma procedure com parâmetros, retornando um Int32, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>Hashtable.</returns>
        public static Hashtable InstanceProcedureOutData(string _connectionKey, string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            Hashtable ht = new Hashtable();
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return null;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                parameter = factory.CreateParameter();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                command.ExecuteNonQuery();

                dbConnection.Close();

                //Recupera os valores de saída e insere numa HashTable
                for (int i = 0; i < _params.Length; i++)
                {
                    ht.Add(_params[i].ParameterName, _params[i].Value);
                }

                return ht;
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return ht;

        }

        #endregion

        #region { InstanceExecuteScalar }

        //Query InstanceExecuteScalar

        /// <summary>
        /// Executa uma query sem parâmetros, retornando um System.Object, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Object</returns>
        public static object InstanceExecuteScalar(string _inQuery, out string _errorMessage)
        {
            object scalar = null;
            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return scalar;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                dbConnection.Open();

                scalar = command.ExecuteScalar();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return scalar;
        }

        /// <summary>
        /// Executa uma query sem parâmetros, retornando um System.Object, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Object</returns>
        public static object InstanceExecuteScalar(string _connectionKey, string _inQuery, out string _errorMessage)
        {
            object scalar = null;
            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return scalar;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                dbConnection.Open();

                scalar = command.ExecuteScalar();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return scalar;
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um System.Object, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Object</returns>
        public static object InstanceExecuteScalar(string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            object scalar = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return scalar = null;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                parameter = factory.CreateParameter();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                scalar = command.ExecuteScalar();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return scalar;
        }

        /// <summary>
        /// Executa uma query com parâmetros, retornando um System.Object, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inQuery">Query de Entrada.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Object</returns>
        public static object InstanceExecuteScalar(string _connectionKey, string _inQuery, out string _errorMessage, params DbParameter[] _params)
        {
            object scalar = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inQuery.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return scalar = null;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inQuery;
                command.CommandType = CommandType.Text;
                command.Connection = dbConnection;

                parameter = factory.CreateParameter();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                scalar = command.ExecuteScalar();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return scalar;
        }

        //Procedure InstanceProcedureExecuteScalar

        /// <summary>
        /// Executa uma StoredProcedure sem parâmetros, retornando um System.Object, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Object</returns>
        public static object InstanceProcedureExecuteScalar(string _inProcedure, out string _errorMessage)
        {
            object scalar = null;
            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return scalar;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                dbConnection.Open();

                scalar = command.ExecuteScalar();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return scalar;
        }

        /// <summary>
        /// Executa uma StoredProcedure sem parâmetros, retornando um System.Object, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <returns>System.Object</returns>
        public static object InstanceProcedureExecuteScalar(string _connectionKey, string _inProcedure, out string _errorMessage)
        {
            object scalar = null;
            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return scalar;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection(_connectionKey);

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                dbConnection.Open();

                scalar = command.ExecuteScalar();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return scalar;
        }

        /// <summary>
        /// Executa uma StoredProcedure com parâmetros, retornando um System.Object, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Object</returns>
        public static object InstanceProcedureExecuteScalar(string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            object scalar = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return scalar = null;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                parameter = factory.CreateParameter();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                scalar = command.ExecuteScalar();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return scalar;
        }

        /// <summary>
        /// Executa uma StoredProcedure com parâmetros, retornando um System.Object, utilizando a conexão do banco.
        /// </summary>
        /// <param name="_connectionKey">Chave de Conexão do AppSettings do web.config.</param>
        /// <param name="_inProcedure">Nome da StoredProcedure.</param>
        /// <param name="_errorMessage">Mensagem de Erro.</param>
        /// <param name="_params">Vetor de DbParameter com os parâmetros.</param>
        /// <returns>System.Object</returns>
        public static object InstanceProcedureExecuteScalar(string _connectionKey, string _inProcedure, out string _errorMessage, params DbParameter[] _params)
        {
            object scalar = null;
            DbParameter parameter = null;

            _errorMessage = String.Empty;

            if (_inProcedure.Length == 0)
            {
                _errorMessage = "Nenhum comando SQL foi informado.";
                return scalar = null;
            }

            try
            {
                //Cria uma conexao na base padrão do AppSettings (Web.config)
                MySqlDBConnectionProvider = new MySqlDBConnectionProvider();
                dbConnection = MySqlDBConnectionProvider.GetConnection();

                factory = MySqlDBConnectionProvider.GetDbFactory();
                command = factory.CreateCommand();

                command.CommandText = _inProcedure;
                command.CommandType = CommandType.StoredProcedure;
                command.Connection = dbConnection;

                parameter = factory.CreateParameter();

                //
                for (int i = 0; i < _params.Length; i++)
                {
                    parameter = _params[i];

                    command.Parameters.Add(parameter);
                }

                dbConnection.Open();

                scalar = command.ExecuteScalar();

                dbConnection.Close();
            }
            catch (DbException exp)
            {
                _errorMessage = "Uma exceção ocorreu durante a execução do comando.  <BR>";
                _errorMessage = _errorMessage + exp.Message;
            }
            finally
            {
                if (command != null)
                {
                    command.Dispose();
                }
                if (dbConnection.State == ConnectionState.Open)
                {
                    dbConnection.Close();
                }
                if (dbConnection != null)
                {
                    dbConnection.Dispose();
                }
            }

            return scalar;
        }

        #endregion
    }
}
