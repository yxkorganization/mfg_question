using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
namespace Mfg.Comm.Db.Sql
{
    public class MongoHelper2
    {
        private MongoClient mongoClient;
        private MongoServer mongoServer;
        private MongoDatabase mongoDataBase;
        private MongoCollection mongoCollection;
        /// <summary>构造方法
        ///
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        /// <param name="tableName">表名</param>
        public MongoHelper2(string connectionString, string tableName)
        {
            this.mongoClient = new MongoClient(ConfigurationManager.ConnectionStrings[connectionString].ConnectionString.Split(new char[]
			{
				'|'
			})[0]);
            this.mongoServer = this.mongoClient.GetServer();
            this.mongoDataBase = this.mongoServer.GetDatabase(ConfigurationManager.ConnectionStrings[connectionString].ConnectionString.Split(new char[]
			{
				'|'
			})[1]);
            this.mongoCollection = this.mongoDataBase.GetCollection(tableName);
        }
        /// <summary>1.0 根据id数组获实体类集合
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="ids">id数组</param>
        /// <param name="_properties">需要的属性字符串（多个属性逗号隔开）</param>
        /// <returns>实体类集合</returns>
        [Obsolete("Use GetAll<T> Instead")]
        public List<T> QueryByIds<T>(int[] ids, params string[] _properties)
        {
            IMongoQuery query = Query.In("_id", new BsonArray(ids));
            FieldsBuilder fields = Fields.Include(_properties);
            MongoCursor<T> source;
            if (_properties.Length == 0)
            {
                source = this.mongoCollection.FindAs<T>(query);
            }
            else
            {
                source = this.mongoCollection.FindAs<T>(query).SetFields(fields);
            }
            return source.ToList<T>();
        }
        /// <summary>1.0 根据id获取单个实体类
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="id">Id</param>
        /// <returns>实体类</returns>
        [Obsolete("Use GetOne<T> Instead")]
        public T QueryById<T>(int id)
        {
            return this.mongoCollection.FindOneByIdAs<T>(id);
        }
        /// <summary>1.0 通过限定的属性值获取实体类
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="pro_name">限定的属性名</param>
        /// <param name="pro_val">限定的属性值</param>
        /// <returns>实体类</returns>
        [Obsolete("Use GetOne<T> Instead")]
        public T QueryByProperty<T, T1>(string pro_name, T1 pro_val)
        {
            IMongoQuery query = Query.EQ(pro_name, BsonValue.Create(pro_val));
            return this.mongoCollection.FindOneAs<T>(query);
        }
        /// <summary>1.0 插入一个实体类
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="user">实体类</param>
        [Obsolete("Use InsertOne<T> Instead")]
        public void InsertByEntity<T>(T user)
        {
            this.mongoCollection.Insert<T>(user);
        }
        /// <summary>1.0 批量插入实体类
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="users">实体类List</param>
        [Obsolete("Use InsertAll<T> Instead")]
        public void InsertBatchByEntities<T>(List<T> users)
        {
            this.mongoCollection.InsertBatch<T>(users);
        }
        /// <summary>1.0 根据id更新数据（支持批量更新属性）
        ///
        /// </summary>
        /// <param name="id">Id</param>
        /// <param name="_property">属性名(多个属性用英文逗号隔开）</param>
        /// <param name="_value">属性值</param>
        [Obsolete("Use UpdateOne<T> Instead")]
        public void UpdateById(int id, string _properties, params object[] _value)
        {
            string[] array = _properties.Split(new char[]
			{
				','
			});
            if (array.Length == _value.Length)
            {
                List<IMongoUpdate> list = new List<IMongoUpdate>();
                for (int i = 0; i < array.Length; i++)
                {
                    list.Add(Update.Set(array[i], BsonValue.Create(_value[i])));
                }
                UpdateBuilder update = Update.Combine(list);
                IMongoQuery query = Query.EQ("_id", id);
                this.mongoCollection.Update(query, update);
            }
        }
        /// <summary>1.0通过属性更新数据（支持批量更新属性）（注意：通过属性的更新结果可能8888是批量的）
        ///
        /// </summary>
        /// <param name="pro_name">限定的属性名</param>
        /// <param name="pro_val">限定的属性值</param>
        /// <param name="_property">要跟新的属性名（多个属性名用英文逗号隔开）</param>
        /// <param name="_value">要跟新的属性值</param>
        [Obsolete("Use UpdateOne<T>/UpdateAll<T> Instead")]
        public void UpdateByProperty<T>(string pro_name, T pro_val, string _properties, params object[] _value)
        {
            string[] array = _properties.Split(new char[]
			{
				','
			});
            if (array.Length == _value.Length)
            {
                List<IMongoUpdate> list = new List<IMongoUpdate>();
                for (int i = 0; i < array.Length; i++)
                {
                    list.Add(Update.Set(array[i], BsonValue.Create(_value[i])));
                }
                UpdateBuilder update = Update.Combine(list);
                IMongoQuery query = Query.EQ(pro_name, BsonValue.Create(pro_val));
                this.mongoCollection.Update(query, update);
            }
        }
        /// <summary>1.0 通过id删除一条数据
        ///
        /// </summary>
        /// <param name="id">Id</param>
        [Obsolete("Use DeleteAll Instead")]
        public void DeleteById(int id)
        {
            IMongoQuery query = Query.EQ("_id", id);
            this.mongoCollection.Remove(query);
        }
        /// <summary>1.0 通过属性删除数据（可能删除多条）
        ///
        /// </summary>
        /// <param name="_property">限定的属性名</param>
        /// <param name="_value">属性值</param>
        [Obsolete("Use DeleteAll Instead")]
        public void DeleteByProperty<T>(string _property, T _value)
        {
            IMongoQuery query = Query.EQ(_property, BsonValue.Create(_value));
            this.mongoCollection.Remove(query);
        }
        /// <summary>1.1 查询单个实体类
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="query">IMongoQuery查询实例</param>
        /// <returns>符合要求的实体类</returns>
        public T GetOne<T>(IMongoQuery query)
        {
            T t = default(T);
            return this.mongoCollection.FindOneAs<T>(query);
        }
        public BsonDocument GetOne(IMongoQuery query)
        {
            return this.mongoCollection.FindOneAs<BsonDocument>(query);
        }
        /// <summary>1.1 查询多个实体类
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="query">IMongoQuery查询实例</param>
        /// <param name="_properties">需要返回的属性名称数组</param>
        /// <returns>符合要求的实体类结果集</returns>
        public List<T> GetAll<T>(IMongoQuery query, params string[] fields)
        {
            List<T> list = new List<T>();
            MongoCursor<T> mongoCursor;
            if (null == query)
            {
                mongoCursor = this.mongoCollection.FindAllAs<T>();
            }
            else
            {
                mongoCursor = this.mongoCollection.FindAs<T>(query);
            }
            if (null != fields)
            {
                mongoCursor.SetFields(fields);
            }
            foreach (T current in mongoCursor)
            {
                list.Add(current);
            }
            return list;
        }
        /// <summary>1.1 查询多个实体类（排序）
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="query">IMongoQuery查询实例</param>
        /// <param name="fields">IMongoSortBy排序实例</param>
        /// <param name="_properties">需要返回的属性名称数组</param>
        /// <returns>符合要求的实体类结果集</returns>
        public List<T> GetAll<T>(IMongoQuery query, IMongoSortBy sortBy, params string[] fields)
        {
            List<T> list = new List<T>();
            MongoCursor<T> mongoCursor;
            if (null == query)
            {
                mongoCursor = this.mongoCollection.FindAllAs<T>();
            }
            else
            {
                mongoCursor = this.mongoCollection.FindAs<T>(query);
            }
            if (null != sortBy)
            {
                mongoCursor.SetSortOrder(sortBy);
            }
            if (null != fields)
            {
                mongoCursor.SetFields(fields);
            }
            foreach (T current in mongoCursor)
            {
                list.Add(current);
            }
            return list;
        }
        /// <summary>1.1 查询多个实体类（排序、分页）
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="query">IMongoQuery查询实例</param>
        /// <param name="fields">IMongoSortBy排序实例</param>
        /// <param name="pageIndex">分页页号</param>
        /// <param name="pageSize">分页页大小</param>
        /// <param name="_properties">需要返回的属性名称数组</param>
        /// <returns>符合要求的实体类结果集</returns>
        public List<T> GetAll<T>(IMongoQuery query, IMongoSortBy sortBy, int pageIndex, int pageSize, params string[] fields)
        {
            List<T> list = new List<T>();
            MongoCursor<T> mongoCursor;
            if (null == query)
            {
                mongoCursor = this.mongoCollection.FindAllAs<T>();
            }
            else
            {
                mongoCursor = this.mongoCollection.FindAs<T>(query);
            }
            if (null != sortBy)
            {
                mongoCursor.SetSortOrder(sortBy);
            }
            if (null != fields)
            {
                mongoCursor.SetFields(fields);
            }
            foreach (T current in mongoCursor.SetSkip((pageIndex - 1) * pageSize).SetLimit(pageSize))
            {
                list.Add(current);
            }
            return list;
        }
        /// <summary>Top函数
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="query">IMongoQuery查询实例</param>
        /// <param name="sortBy">IMongoSortBy排序实例</param>
        /// <param name="topCount">Top条数</param>
        /// <param name="filed"></param>
        /// <returns></returns>
        public List<T> Top<T>(IMongoQuery query, IMongoSortBy sortBy, int topCount, params string[] fields)
        {
            List<T> list = new List<T>();
            MongoCursor<T> mongoCursor;
            if (null == query)
            {
                mongoCursor = this.mongoCollection.FindAllAs<T>().SetSortOrder(sortBy).SetLimit(topCount);
            }
            else
            {
                mongoCursor = this.mongoCollection.FindAs<T>(query).SetSortOrder(sortBy).SetLimit(topCount);
            }
            if (null != fields)
            {
                mongoCursor.SetFields(fields);
            }
            foreach (T current in mongoCursor)
            {
                list.Add(current);
            }
            return list;
        }
        /// <summary>更具查询条件获取Distinct数量
        ///
        /// </summary>
        /// <param name="key">要distinct的属性名称</param>
        /// <param name="query">IMongoQuery查询实例</param>
        /// <returns>数量</returns>
        public int Distinct(string key, IMongoQuery query)
        {
            return this.mongoCollection.Distinct(key, query).Count<BsonValue>();
        }
        /// <summary>根据查询条件获取Count数量
        ///
        /// </summary>
        /// <param name="query">IMongoQuery实例</param>
        /// <returns>数量</returns>
        public long Count(IMongoQuery query)
        {
            return this.mongoCollection.Count(query);
        }
        /// <summary>分组
        ///
        /// </summary>
        /// <param name="query">IMongoQuery 查询实例</param>
        /// <param name="keys">需要分组的键</param>
        /// <param name="initial">初始化函数（javascript）</param>
        /// <param name="reduce">分组内执行函数（javascript）</param>
        /// <param name="finalize">每个分组完成后执行函数（javascript）</param>
        /// <returns>可枚举的BsonDocument</returns>
        public IEnumerable<BsonDocument> Group(IMongoQuery query, IMongoGroupBy keys, BsonDocument initial, BsonJavaScript reduce, BsonJavaScript finalize)
        {
            return this.mongoCollection.Group(query, keys, initial, reduce, finalize);
        }
        public AggregateResult Aggregate(params BsonDocument[] pipeline)
        {
            return this.mongoCollection.Aggregate(pipeline);
        }
        /// <summary>1.1 插入一个实体类
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="entity">实体类实例</param>
        /// <returns>WriteConcernResult实例</returns>
        public WriteConcernResult InsertOne<T>(T entity)
        {
            WriteConcernResult result;
            try
            {
                result = this.mongoCollection.Insert<T>(entity);
            }
            catch (Exception var_1_12)
            {
                result = null;
            }
            return result;
        }
        public IEnumerable<WriteConcernResult> InsertAll<T>(IEnumerable<T> entitys)
        {
            IEnumerable<WriteConcernResult> result;
            try
            {
                result = this.mongoCollection.InsertBatch<T>(entitys);
            }
            catch (Exception var_1_12)
            {
                result = null;
            }
            return result;
        }
        /// <summary>1.1 替换一个实体类
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="entity">实体类实例</param>
        /// <returns>WriteConcernResult实例</returns>
        public WriteConcernResult UpdateOne<T>(T entity)
        {
            return this.mongoCollection.Save<T>(entity);
        }
        /// <summary>1.1 更新一个实体类
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="query">IMongoQuery查询实例</param>
        /// <param name="update">IMongoUpdate更新实例</param>
        /// <returns>WriteConcernResult实例</returns>
        public WriteConcernResult UpdateOne(IMongoQuery query, IMongoUpdate update)
        {
            return this.mongoCollection.Update(query, update);
        }
        /// <summary>1.1 更新一个实体类
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="query">IMongoQuery查询实例</param>
        /// <param name="update">IMongoUpdate更新实例</param>
        /// <param name="options">MongoUpdateOptions 更新操作实例</param>
        /// <returns>WriteConcernResult实例</returns>
        public WriteConcernResult UpdateOne(IMongoQuery query, IMongoUpdate update, MongoUpdateOptions options)
        {
            return this.mongoCollection.Update(query, update, options);
        }
        public WriteConcernResult UpdateOne(IMongoQuery query, IMongoUpdate update, UpdateFlags flags)
        {
            return this.mongoCollection.Update(query, update, flags);
        }
        /// <summary>1.1 更新多个实体类
        ///
        /// </summary>
        /// <typeparam name="T">实体类型</typeparam>
        /// <param name="query">IMongoQuery查询实例</param>
        /// <param name="update">IMongoUpdate更新实例</param>
        /// <returns>WriteConcernResult实例</returns>
        public WriteConcernResult UpdateAll(IMongoQuery query, IMongoUpdate update)
        {
            return this.mongoCollection.Update(query, update, UpdateFlags.Multi);
        }
        /// <summary>1.1 删除实体类
        ///
        /// </summary>
        /// <param name="query">IMongoQuery查询实例</param>
        /// <returns>WriteConcernResult实例</returns>
        public WriteConcernResult DeleteAll(IMongoQuery query)
        {
            return this.mongoCollection.Remove(query);
        }
    }
}
