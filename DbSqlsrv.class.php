<?php
/**
 * 存在问题：
 *         update delete 不支持 limit()
 */


/**
 * Sqlsrv数据库驱动
 * @package     Db
 * @subpackage  Driver
 * @author      FYCDC-Germ   <hzfycdc@126.com>
  */
class DbSqlsrv extends Db
{
    //是否连接
    static protected $isConnect = false;
    //数据库连接
    public $link = null;

    //获得数据库连接
    public function connectDb()
    {
        if (!self::$isConnect) {		
			$config  =  array(
				'hostname'=>C("DB_HOST"),
				'hostport'=>C("DB_PORT"),
				'database'=>C("DB_DATABASE"),
				'username'=>C("DB_USER"),
				'password'=>C("DB_PASSWORD"),
				'CharacterSet' => C("DB_CHARSET")
				);
            $host = $config['hostname'].($config['hostport']?",{$config['hostport']}":'');
            $connectInfo  =  array('Database'=>$config['database'],'UID'=>$config['username'],'PWD'=>$config['password'],'CharacterSet' => C('DB_CHARSET'));
            $link = sqlsrv_connect( $host, $connectInfo);
            //连接错误
            if (!$link) {
                return false;
            }
            self::$isConnect = $link;
            //self::setCharset();
        }
        $this->link = self::$isConnect;
        return true;
    }

    /**
     * 设置字符集  应该可以不用！ 连接时已加入字符编码
     */
    static private function setCharset()
    {
        self::$isConnect->set_charset(C("DB_CHARSET"));
    }

    /**
     * limit 操作
     * @param mixed $data
     * @return type
     */
    public function limit($data)
    {
        $tops='';
        $wheres='';
        if(empty($data)){
            $this->opt['limit']=' ';
        }elseif(is_numeric($data)){
            $tops = ' top '.$data .' ';
            $this->opt['limit']=' ';
        }else if(is_array($data)){
            $tops = ' top '.$data[1].' ';
            $wheres = " {$this->opt['pri']} >= {$data[0]} ";
            $this->opt['limit']=' ';
        }elseif(stristr($data,',')){
            $arr=explode(',',$data);
            $tops = ' top '. $arr[1].' ';
            $wheres =" {$this->opt['pri']} >={$data[0]} ";
            $this->opt['limit']=' ';
        }       
        if(!empty($tops) && !stristr($this->opt['field'],' top ')){
            $this->opt['field']= $tops.$this->opt['field'];
        } 
        if(!empty($wheres)){
           /* if(empty($this->opt['where'])){
                $this->opt['where']=' WHERE '.$wheres;
            }else{
                $this->opt['where']=' AND '.$wheres;
            }*/
            $this->where($wheres);
        }
    }

    /**
     * 给出最后插入ID
     */
	public function mssql_insert_id() {
        $query  =   "SELECT @@IDENTITY as last_insert_id";
        $result =   sqlsrv_query($this->link,$query);
        $row  =   sqlsrv_fetch_array($result);
        sqlsrv_free_stmt($result);
        return $row['last_insert_id'];
    }
	/**
     * 给出最后插入ID
     */
	public function getInsertId(){
		return $this->mssql_insert_id();
	}
	
	public function getAffectedRows(){
		return sqlsrv_rows_affected($this->lastQuery);
	}
	/**
     * 给出服务器版本
     */
	public function getVersion(){
		$server_info = sqlsrv_server_info($this->link);
        return $server_info['SQLServerVersion'];
	}

    /**
     * 给出客户端libary版本
     */
    public function client_version()
    {
        $client_info = sqlsrv_client_info($this->link);
        return $client_info['DriverODBCVer'] . ' ' . $client_info['DriverVer'];
    }

    //遍历结果集(根据INSERT_ID)
    protected function fetch()
    {
        $res = sqlsrv_fetch_array($this->lastQuery,SQLSRV_FETCH_ASSOC);
        if (!$res) {
            $this->resultFree();
        }
        return $res;
    }
    //数据安全处理
    public function escapeString($str)
    {       
            //return str_replace("'", "''", $str);//
        return addslashes($str);
    }
    /**
     * 给出表字段
     */
	public function getTableFields($tableName) {

        $name = C('DB_DATABASE') . '.' . $tableName;
        //字段缓存
        if (!DEBUG && F($name, false, APP_TABLE_PATH)) {
            $f = F($name, false, APP_TABLE_PATH);
        } else {
            $result = $this->query("
                SELECT column_name,data_type,column_default,is_nullable
                FROM   information_schema.tables AS t
                JOIN   information_schema.columns AS c
                ON     t.table_catalog = c.table_catalog
                AND    t.table_schema  = c.table_schema
                AND    t.table_name    = c.table_name
                WHERE  t.table_name = '{$tableName}'");
            $pk = $this->query("SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_NAME='{$tableName}'");
           // $info = array();
            $f=array();
            $pri = $pk[0]['COLUMN_NAME'];
            if($result) {
                foreach ($result as $key => $val) {
                    $fieldname=$val['column_name'];

                    $f[$fieldname] = array(
                        'field'   => $fieldname,
                        'name'    => $fieldname,
                        'type'    => $val['data_type'],
                        'notnull' => (bool) ($val['is_nullable'] === ''), // not null is empty, null is yes
                        'default' => $val['column_default'],
                        'primary' => $fieldname == $pk[0]['COLUMN_NAME'],
                        'null'    =>  (bool) ($val['is_nullable'] === ''),
                        'key'     =>  $fieldname == $pk[0]['COLUMN_NAME'],
                        'pri'     => $fieldname == $pk[0]['COLUMN_NAME'],
                        'autoinc' => false,
                    );
                }
                DEBUG && F($name, $f, APP_TABLE_PATH);
            }
        }    
        //$info = 
        return array('fields'=>$f,'pri'=>$pri,'fieldData'=>$f,'table'=>$tableName);
        //p($info);
        //return $info;
    }
	 public function getTables($dbName='') {
        $result   =  $this->query("SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE = 'BASE TABLE'
            ");
        $info   =   array();
        foreach ($result as $key => $val) {
            $info[$key] = current($val);
        }
        return $info;
    }

    /**
     * 格式化SQL操作参数 字段加上标识符  值进行转义处理
     * @param array $vars 处理的数据
     * @return array
     */
    public function formatField($vars)
    {
        //格式化的数据
        $data = array();
        foreach ($vars as $k => $v) {
            //校验字段与数据
            if ($this->isField($k)) {
                $data['fields'][] = "[" . $k . "]";
                $v = $this->escapeString($v);
                $default = $this->opt['fieldData'][$k]['default'];
                $type = $this->opt['fieldData'][$k]['type'];
                $data['values'][] = empty($v) && is_null($default) && $type != 'text' ? "null" : "'{$v}'";
            }
        }
        return $data;
    }

    //执行SQL没有返回值
    public function exe($sql,$bind=array()) 
    {
        //查询参数初始化
        $this->optInit();
        //将SQL添加到调试DEBUG
        $this->debug($sql);
		//$str    =   str_replace(array_keys($bind),'?',$str);
        //p($sql);//打印出SQL语句
        //$sql = str_replace('"',"'",$sql);//mssql 不支持双引号
        $bind   =   array_values($bind);
        $this->lastQuery = sqlsrv_query($this->link,$sql,$bind);
        if ($this->lastQuery) {
            //自增id
            $insert_id = $this->mssql_insert_id();
            return $insert_id ? $insert_id : true;
        } else {
            $this->error($this->link->error . "\t" . $sql);
            return false;
        }
    }

    //发送查询 返回数组
    public function query($sql){
        $cache_time = $this->cacheTime ? $this->cacheTime : intval(C("CACHE_SELECT_TIME"));
        $cacheName = md5($sql . MODULE . CONTROLLER . ACTION);
        if ($cache_time >= 0) {
            $result = S($cacheName, FALSE, null, array("Driver" => "file", "dir" => APP_CACHE_PATH, "zip" => false));
            if ($result) {
                //查询参数初始化
                $this->optInit();
                return $result;
            }
        }
        //SQL发送失败
        if (!$this->exe($sql))
            return false;
        $list = array();
        while (($res = $this->fetch()) != false) {
            $list [] = $res;
        }
        if ($cache_time >= 0 && count($list) <= C("CACHE_SELECT_LENGTH")) {
            S($cacheName, $list, $cache_time, array("Driver" => "file", "dir" => APP_CACHE_PATH, "zip" => false));
        }
        return empty($list) ? NULL : $list;
    }

    //释放结果集
    protected function resultFree()
    {
        if (isset($this->lastQuery)) {
            sqlsrv_free_stmt($this->lastQuery);
            //$this->lastQuery->null;
        }
    }


    //自动提交模式true开启false关闭
    public function beginTrans()
    {
        if ( !$this->link ) return false;
        $stat = func_get_arg(0);        
        if(!$stat){
            sqlsrv_begin_transaction($this->link);
        }
        return ;
    }

    //提供一个事务
    public function commit()
    {
        return sqlsrv_commit($this->link);
    }

    //回滚事务
    public function rollback()
    {
            return sqlsrv_rollback($this->link);
    }

    // 释放连接资源
    public function close()
    {
        if ($this->link){
            sqlsrv_close($this->link);
        }
        $this->link = null;
    }

    //析构函数  释放连接资源
    public function __destruct()
    {
        $this->close();
    }


}
