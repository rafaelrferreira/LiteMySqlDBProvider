using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Data.Common;
using System.Data;
using System.Collections;
using System.Runtime.InteropServices;

using MySql.Data.MySqlClient;

namespace LiteMySqlDBProvider.MySqlDataAccessLayer
{
    public struct MySqlDbParametro
    {
        private string nome;
        private object valor;
        private int tam;
        private DbType tipo;
        private DbParameter inout;
        private static DbProviderFactory factory;

        public string Nome
        {
            get { return nome; }
            set { nome = value; }
        }
        public object Valor
        {
            get { return valor; }
            set { valor = value; }
        }
        public int Tam
        {
            get { return tam; }
            set { tam = value; }
        }
        public DbType Tipo
        {
            get { return tipo; }
            set { tipo = value; }
        }
        public DbParameter Inout
        {
            get { return inout; }
            set { inout = value; }
        }

        /// <summary>
        /// Método que cria e retorna um parâmetro MySqlParameter 
        /// </summary>
        /// <param name="_nome">Nome do parâmetro.</param>
        /// <param name="_valor">Valor do parâmetro.</param>
        /// <param name="_tipo">Tipo do parâmetro (MySqlDbType).</param>
        public static MySqlParameter Parametro(string _nome, object _valor, MySqlDbType _tipo)
        {
            MySqlParameter parameter = new MySqlParameter();

            parameter.ParameterName = _nome;
            parameter.Value = _valor;
            parameter.MySqlDbType = _tipo;

            return parameter;
        }

        /// <summary>
        /// Método que cria e retorna um parâmetro MySqlParameter.
        /// </summary>
        /// <param name="_nome">Nome do parâmetro.</param>
        /// <param name="_valor">Valor do parâmetro.</param>
        /// <param name="_tipo">Tipo do parâmetro (MySqlDbType).</param>
        /// <param name="_direction">Direção do parâmetro (ParameterDirection).</param>
        public static MySqlParameter Parametro(string _nome, object _valor, MySqlDbType _tipo, ParameterDirection _direction)
        {
            MySqlParameter parameter = new MySqlParameter();

            parameter.ParameterName = _nome;
            parameter.Value = _valor;
            parameter.MySqlDbType = _tipo;
            parameter.Direction = _direction;

            return parameter;
        }
    }
}
