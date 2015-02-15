<?php
/**
 * Redis Queue Job Dispatcher
 */
class JobRedis extends Redis{

	/**
	 * Place to store running job ids
	 */
	const KEY_JOB_ID = 'jobs_id';

	/**
	 * Place to store all queued jobs info
	 */
	const KEY_JOB_LOAD = 'jobs_load';

	/**
	 * Place to store jobs results
	 */
	const KEY_JOB_RESULT = 'jobs_result';

	/**
	 * Place to store all the orders
	 */
	const KEY_ORDER_QUEUE = 'jobs_order';

	/**
	 * Default socket timeout
	 * @var int
	 */
	const DEFAULT_SOCKET_TIMEOUT = 60;

	/**
	 * Max locking time for same job
	 * @var int
	 */
	const MAX_LOCK_TIME = 60;

	/**
	 * Max time for sync read of the result
	 * @var int
	 */
	const MAX_READ_TIME = 2;

	/**
	 * Max connecting time
	 * @var int
	 */
	const MAX_CONNECT_TIME = 3;

	/**
	 * Max blocking seconds for waiting a job
	 * @var int
	 */
	const MAX_BLOCK_TIME = 1;

	/**
	 * Whether redis is connected
	 * @var bool
	 */
	private $is_connected = false;

	public function __construct($host, $port){
		if(empty($host) or empty($port)){
			$this->is_connected = false;
		}else{
			ini_set('default_socket_timeout', self::DEFAULT_SOCKET_TIMEOUT);
			try{
				parent::__construct();
				$flag = $this->connect($host, $port, self::MAX_CONNECT_TIME);
				if($flag){
					$this->is_connected = true;
				}
			}catch(RedisException $e){
				$this->is_connected = false;
			}
		}
	}

	public function __destruct(){
		try{
			$this->close();
		}catch(RedisException $e){
			$this->is_connected = false;
		}
	}

	/**
	 * whether still connected
	 * @return bool
	 */
	public function isConnected(){
		return $this->is_connected;
	}

	/**
	 * Send order
	 * 
	 * @param mixed $order order details
	 * @return bool
	 */
	public function sendOrder($order){
		if(empty($order) or !$this->is_connected){
			return false;
		}
		try{
			$ret = $this->lpush(self::KEY_ORDER_QUEUE, serialize($order));
			return $ret ? true : false;
		}catch(RedisException $e){
			$this->is_connected = false;
			return false;
		}
	}

	/**
	 * Receive order
	 * @return array order details
	 */
	public function receiveOrder(){
		if(!$this->is_connected){
			return false;
		}
		try{
			$order = $this->brpop(self::KEY_ORDER_QUEUE, self::MAX_BLOCK_TIME);
			if(empty($order) or !is_array($order) or count($order) != 2){
				return false;
			}
			$order = unserialize(array_pop($order));
			return $order ? $order : false;
		}catch(RedisException $e){
			$this->is_connected = false;
			return false;
		}
	}

	/**
	 * Send a job to queue
	 *
	 * @param string $func queue function name
	 * @param mixed $load params to call function
	 * @param bool $sync sync or not，default: false
	 * @return mixed
	 */
	public function sendJob($func, $load = '', $sync = false){
		if(empty($func) or !$this->is_connected){
			return false;
		}
		$sync = !empty($sync) ? true : false;
		$unique = sprintf('%u', crc32(serialize($load)));
		$work_load = array(
			'unique' => $unique,
			'load' => $load,
			'sync' => $sync,
		);
		try{
			$lock_key = self::KEY_JOB_ID . ':' . $func . ':' . $unique;
			$job_exist = $this->exists($lock_key);
			if(!$job_exist){
				$this->multi(Redis::PIPELINE)
							->setex($lock_key, self::MAX_LOCK_TIME, 'locked')
							->lpush(self::KEY_JOB_LOAD . ':' . $func, serialize($work_load))
							->exec();
			}
			if($sync){
				$result_key = self::KEY_JOB_RESULT . ':' . $func . ':' . $unique;
				$result = $this->brpoplpush($result_key, $result_key, self::MAX_BLOCK_TIME);
				return $result ? unserialize($result) : false;
			}
		}catch(RedisException $e){
			$this->is_connected = false;
			return false;
		}
		return true;
	}

	/**
	 * Receive job from queue
	 * 
	 * @param string $func queue function name
	 * @return bool
	 */
	public function receiveJob($func){
		if(empty($func) or !$this->is_connected or !function_exists($func)){
			return false;
		}
		try{
			$work_load = $this->brpop(self::KEY_JOB_LOAD . ':' . $func, self::MAX_BLOCK_TIME);
			if(empty($work_load) or !is_array($work_load) or count($work_load) != 2){
				return false;
			}
			$work_load = unserialize(array_pop($work_load));
			$unique = isset($work_load['unique']) && !empty($work_load['unique']) ? $work_load['unique'] : '';
			if(empty($unique)){
				return false;
			}
			$load = isset($work_load['load']) && !empty($work_load['load']) ? $work_load['load'] : '';
			$sync = isset($work_load['sync']) && !empty($work_load['sync']) ? true : false;
			$result = $func($load);
			$lock_key = self::KEY_JOB_ID . ':' . $func . ':' . $unique;
			if($sync){
				$result_key = self::KEY_JOB_RESULT . ':' . $func . ':' . $unique;
				$this->multi(Redis::PIPELINE)
							->lpush($result_key, serialize($result))
							->expire($result_key, self::MAX_READ_TIME)
							->del($lock_key)
							->exec();
			}else{
				$this->del($lock_key);
			}
		}catch(RedisException $e){
			$this->is_connected = false;
			return false;
		}
		return true;
	}

}