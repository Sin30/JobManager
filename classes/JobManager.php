<?php

declare(ticks = 1);

error_reporting(E_ALL | E_STRICT);

date_default_timezone_set('Asia/Shanghai');

require_once("JobRedis.php");

/**
 * Class that handles all the process management
 */
class JobManager {

    /**
     * Log levels can be enabled from the command line with -v, -vv, -vvv, -vvvv
     */
    const LOG_LEVEL_INFO = 1;
    const LOG_LEVEL_PROC_INFO = 2;
    const LOG_LEVEL_WORKER_INFO = 3;
    const LOG_LEVEL_DEBUG = 4;
    const LOG_LEVEL_CRAZY = 5;

    /**
     * Default config section name
     */
    const DEFAULT_CONFIG = "JobManager";

    /**
     * The config filename
     */
    protected $config_file;

    /**
     * Holds the worker configuration
     */
    protected $config = array();

    /**
     * Boolean value that determines if the running code is the parent or a child
     */
    protected $isparent = true;

    /**
     * When true, workers will stop look for jobs and the parent process will
     * kill off all running children
     */
    protected $stop_work = false;

    /**
     * The filename to log to
     */
    protected $log_file;

    /**
     * Holds the resource for the log file
     */
    protected $log_file_handle;

    /**
     * Verbosity level for the running script. Set via -v option
     */
    protected $verbose = 0;

    /**
     * The array of running child processes
     */
    protected $children = array();

    /**
     * The PID of the running process. Set for parent and child processes
     */
    protected $pid = 0;

    /**
     * PID file for the parent process
     */
    protected $pid_file = "";

    /**
     * Directory where worker functions are found
     */
    protected $worker_dir = "";

    /**
     * Number of workers for every job
     */
    protected $job_count = 0;

    /**
     * Maximum time a worker will run
     */
    protected $max_worker_lifetime = 3600;

    /**
     * Maximum job iterations per worker
     */
    protected $max_runs_per_worker = null;

    /**
     * Servers that workers connect to
     */
    protected $servers = array();

    /**
     * List of functions available for work
     */
    protected $functions = array();

    /**
     * Creates the manager and gets things going
     *
     */
    public function __construct() {

        if(!function_exists("posix_kill")){
            $this->show_help("Please ensure POSIX functions are installed");
        }

        if(!function_exists("pcntl_fork")){
            $this->show_help("Please ensure Process Control functions are installed");
        }

        $this->pid = getmypid();

        $this->getopt();

        $this->register_ticks();

        $this->load_workers();

        if(empty($this->functions)){
            $this->log("No workers found");
            exit();
        }

        $this->bootstrap();

        while(!$this->stop_work) {

            $this->process_loop();

        }

        $this->log("Exiting");
    }

	/**
	 * Main process loop
	 */
    protected function process_loop(){
        static $redis = null;
        if($redis == null){
            $redis = $this->init_redis();
            $this->log("Redis is up.", self::LOG_LEVEL_INFO);
        }
        if(!$redis->isConnected()){
            $this->log("Redis is down.", self::LOG_LEVEL_INFO);
            /**
             * it may not connect again, so stop children and try.
             */
            $this->stop_children();
            $redis = $this->init_redis();
        }

        $order = $redis->receiveOrder();
        if(empty($order) or !isset($order['name'])){
            return;
        }

        $pid = isset($order['pid']) ? $order['pid'] : '';
        $worker = isset($order['worker']) ? $order['worker'] : '';
        switch ($order['name']) {
            case 'start_child':
                $this->log("Order to start child ({$worker})", self::LOG_LEVEL_PROC_INFO);
                if(!$this->stop_work){
                    $this->fork_worker($worker);
                }
                break;
            case 'stop_child':
                $this->log("Order to stop child {$pid} ({$worker})", self::LOG_LEVEL_PROC_INFO);
                if(!empty($this->children[$pid])){
                    posix_kill($pid, SIGTERM);
                    unset($this->children[$pid]);
                }
                break;
            case 'restart_child':
                $this->log("Order to restart child {$pid} ({$worker})", self::LOG_LEVEL_PROC_INFO);
                if(!empty($this->children[$pid])){
                    if(!$this->stop_work){
                        $this->fork_worker($this->children[$pid]);
                    }
                    posix_kill($pid, SIGTERM);
                    unset($this->children[$pid]);
                }
                break;
            case 'stop_work':
                $this->log("Order to stop work", self::LOG_LEVEL_PROC_INFO);
                $this->stop_work = true;
                $this->stop_children();
                break;
            case 'child_started':
                $this->log("Child started {$pid} ($worker)", self::LOG_LEVEL_PROC_INFO);
                $this->children[$pid] = $worker;
                break;
            default:
                $this->log("unknown order name : {$order['name']}", self::LOG_LEVEL_PROC_INFO);
                break;
        }
        $redis = $this->init_redis();
    }

    public function __destruct() {
        if($this->isparent){
            if(!empty($this->pid_file) && file_exists($this->pid_file)){
                if(!unlink($this->pid_file)) {
                    $this->log("Could not delete PID file", self::LOG_LEVEL_PROC_INFO);
                }
            }
        }
    }

	/**
	 * Initialize redis instance
	 * @return JobRedis
	 */
    protected function init_redis(){
        list($host, $port) = explode(':', $this->servers);
        if(empty($host) or empty($port)){
            $this->log("Redis config error : {$this->servers}", self::LOG_LEVEL_PROC_INFO);
            $this->show_help("Redis config error : {$this->servers}");
        }
        $redis = new JobRedis($host, $port);
        if(!$redis->isConnected()){
            $this->log("Redis connect error : {$this->servers}", self::LOG_LEVEL_PROC_INFO);
            $this->show_help("Redis connect error : {$this->servers}");
        }
        return $redis;
    }

	/**
	 * Get options passing through console
	 */
    protected function getopt() {
        $opts = getopt("c:dD:h:Hl:P:v::w:r:x:Z");

        if(isset($opts['H'])){
            $this->show_help();
        }

        if(isset($opts['c'])){
            $this->config_file = $opts['c'];
        }else{
            $this->config_file = './config.ini';
        }

        if(!file_exists($this->config_file)){
            $this->show_help("Config file {$this->config_file} not found.");
        }else{
            $this->parse_config($this->config_file);
        }

        /**
         * command line opts always override config file
         */
        if (isset($opts['P'])) {
            $this->config['pid_file'] = $opts['P'];
        }

        if(isset($opts["l"])){
            $this->config['log_file'] = $opts['l'];
        }

        if (isset($opts['w'])) {
            $this->config['worker_dir'] = $opts['w'];
        }

        if (isset($opts['x'])) {
            $this->config['max_worker_lifetime'] = (int)$opts['x'];
        }

        if (isset($opts['r'])) {
            $this->config['max_runs_per_worker'] = (int)$opts['r'];
        }

        if (isset($opts['D'])) {
            $this->config['count'] = (int)$opts['D'];
        }

        if (isset($opts['h'])) {
            $this->config['host'] = $opts['h'];
        }

        /**
         * If we want to daemonize, fork here and exit
         */
        if(isset($opts["d"])){
            $pid = pcntl_fork();
            if($pid>0){
                $this->isparent = false;
                exit();
            }
            $this->pid = getmypid();
            posix_setsid();
        }

        if(!empty($this->config['log_file'])){
            $this->log_file = $this->config['log_file'];
            $this->open_log_file($this->log_file);
        }
        
        if(!empty($this->config['pid_file'])){
            $fp = @fopen($this->config['pid_file'], "w");
            if($fp){
                fwrite($fp, $this->pid);
                fclose($fp);
            } else {
                $this->show_help("Unable to write PID to {$this->config['pid_file']}");
            }
            $this->pid_file = $this->config['pid_file'];
        }

        if(isset($opts["v"])){
            switch($opts["v"]){
                case false:
                    $this->verbose = self::LOG_LEVEL_INFO;
                    break;
                case "v":
                    $this->verbose = self::LOG_LEVEL_PROC_INFO;
                    break;
                case "vv":
                    $this->verbose = self::LOG_LEVEL_WORKER_INFO;
                    break;
                case "vvv":
                    $this->verbose = self::LOG_LEVEL_DEBUG;
                    break;
                case "vvvv":
                default:
                    $this->verbose = self::LOG_LEVEL_CRAZY;
                    break;
            }
        }

        if(!empty($this->config['worker_dir'])){
            $this->worker_dir = $this->config['worker_dir'];
        } else {
            $this->worker_dir = "./workers";
        }

        $dirs = explode(",", $this->worker_dir);
        foreach($dirs as &$dir){
            $dir = trim($dir);
            if(!file_exists($dir)){
                $this->show_help("Worker dir ".$dir." not found");
            }
        }
        unset($dir);

        if(!empty($this->config['max_worker_lifetime'])){
            $this->max_worker_lifetime = (int)$this->config['max_worker_lifetime'];
        }

        if(!empty($this->config['max_runs_per_worker'])){
            $this->max_runs_per_worker = (int)$this->config['max_runs_per_worker'];
        }

        if(!empty($this->config['count'])){
            $this->job_count = (int)$this->config['count'];
        }

        if(!empty($this->config['host'])){
            $this->servers = $this->config['host'];
        } else {
            $this->show_help("Host config not found.");
        }

        if (!empty($this->config['include']) && $this->config['include'] != "*") {
            $this->config['include'] = explode(",", $this->config['include']);
        } else {
            $this->config['include'] = array();
        }

        if (!empty($this->config['exclude'])) {
            $this->config['exclude'] = explode(",", $this->config['exclude']);
        } else {
            $this->config['exclude'] = array();
        }

        /**
         * Debug option to dump the config and exit
         */
        if(isset($opts["Z"])){
            print_r($this->config);
            exit();
        }
    }

	/**
	 * Open log file handler
	 * @param $file
	 */
    protected function open_log_file($file) {
        if ($this->log_file_handle) {
            @fclose($this->log_file_handle);
        }
        $this->log_file_handle = @fopen($file, "a");
        if(!$this->log_file_handle){
            $this->show_help("Could not open log file $file");
        }
    }

	/**
	 * Parse config from file
	 * @param $file
	 */
    protected function parse_config($file) {
        $config = parse_ini_file($file, true);
        if(empty($config)){
            $this->show_help("No configuration found in $file");
        }
        if (isset($config[self::DEFAULT_CONFIG])) {
            $this->config = $config[self::DEFAULT_CONFIG];
            $this->config['functions'] = array();
        }
        foreach($config as $function=>$data){
            if (strcasecmp($function, self::DEFAULT_CONFIG) != 0) {
                $this->config['functions'][$function] = $data;
            }
        }
    }

	/**
	 * Load workers to cache
	 */
    protected function load_workers() {
        $this->functions = array();
        $dirs = explode(",", $this->worker_dir);
        foreach($dirs as $dir){
            $this->log("Loading workers in ".$dir);
            $worker_files = glob($dir."/*.php");
            if (!empty($worker_files)) {
                foreach($worker_files as $file){
                    $function = substr(basename($file), 0, -4);
                    if (!empty($this->config['include'])) {
                        if (!in_array($function, $this->config['include'])) {
                            continue;
                        }
                    }
                    if (in_array($function, $this->config['exclude'])) {
                        continue;
                    }
                    if(!isset($this->functions[$function])){
                        $this->functions[$function] = array();
                    }
                    $func_count = 0;
                    if(isset($this->config['functions'][$function]['count'])){
                        $func_count = (int)($this->config['functions'][$function]['count']);
                    }
                    if(!empty($func_count)){
                        $this->functions[$function]["count"] = $func_count;
                    }else{
                        $this->functions[$function]["count"] = max($this->job_count, 1);
                    }
                    $this->functions[$function]['path'] = $file;
                }
            }
        }
    }

	/**
	 * boot up all workers
	 */
    protected function bootstrap() {
        $function_count = array();
        foreach($this->functions as $worker=>$config) {
            if(empty($function_count[$worker])){
                $function_count[$worker] = 0;
            }
            while($function_count[$worker] < $config["count"]){
                $this->fork_worker($worker);
                $function_count[$worker]++;;
            }
            /**
             * php will eat up your cpu if you don't have this
             */
            usleep(50000);
        }
    }

	/**
	 * Fork a new worker
	 * @param $worker
	 * @return bool
	 */
    protected function fork_worker($worker) {
        if(empty($worker)){
            return false;
        }
        $pid = pcntl_fork();
        switch($pid) {
            case 0:
                $this->isparent = false;
                $this->register_ticks(false);
                $this->pid = getmypid();
                if($this->validate_worker($worker)){
                    $this->run_worker($worker);
                    $this->log("Child exiting {$this->pid} ($worker)", self::LOG_LEVEL_WORKER_INFO);
                }else{
                    $this->log("Child invalid {$this->pid} ($worker)", self::LOG_LEVEL_WORKER_INFO);
                }
                exit();
                break;
            case -1:
                $this->log("Could not fork");
                $this->stop_work = true;
                $this->stop_children();
                break;
            default:
                break;
        }
        return true;
    }

	/**
	 * Check worker
	 * @param $worker
	 * @return bool
	 */
    protected function validate_worker($worker) {
        if(!isset($this->functions[$worker])){
            $this->load_workers();
        }
        $config = isset($this->functions[$worker]) ? $this->functions[$worker] : array();
        if(!isset($config['path']) or empty($config['path'])){
            return false;
        }
        include_once $config['path'];
        if(!function_exists($worker)){
            $this->log("Function $worker not found in " . $config['path']);
            return false;
        }
        return true;
    }

	/**
	 * Start worker
	 * @param $worker
	 */
    protected function run_worker($worker) {
        $execution_count = 0;
        $start = time();
        $run_out = false;
        $redis = $this->init_redis();
        $order = array(
            'name' => 'child_started',
            'pid' => $this->pid,
            'worker' => $worker,
        );
        $redis->sendOrder($order);

        while(!$this->stop_work and !$run_out and $redis->isConnected()){
            $redis->receiveJob($worker);
            if($this->max_worker_lifetime > 0 && time() - $start > $this->max_worker_lifetime) {
                $this->log("Reach {$this->max_worker_lifetime} seconds, exiting", self::LOG_LEVEL_WORKER_INFO);
                $run_out = true;
            }
            if(!empty($this->max_runs_per_worker) && $execution_count >= $this->max_runs_per_worker) {
                $this->log("Reach {$execution_count} jobs, exiting", self::LOG_LEVEL_WORKER_INFO);
                $run_out = true;
            }
            $execution_count++;
        }

        if(!$this->stop_work){
            $order = array(
                'name' => 'restart_child',
                'pid' => $this->pid,
                'worker' => $worker,
            );
            $redis->sendOrder($order);
        }
    }

	/**
	 * Stop children workers by signal
	 * @param int $signal
	 */
    protected function stop_children($signal=SIGTERM) {
        $this->log("Stopping children", self::LOG_LEVEL_PROC_INFO);
        foreach($this->children as $pid=>$worker){
            $this->log("Stopping child $pid ($worker)", self::LOG_LEVEL_PROC_INFO);
            posix_kill($pid, $signal);
        }
    }

	/**
	 * Register signal handlers
	 * @param bool $parent
	 */
    protected function register_ticks($parent=true) {
        if($parent){
            $this->log("Registering signals for parent", self::LOG_LEVEL_DEBUG);
            pcntl_signal(SIGTERM, array($this, "signal"));
            pcntl_signal(SIGINT,  array($this, "signal"));
            pcntl_signal(SIGHUP,  array($this, "signal"));
        } else {
            $res = pcntl_signal(SIGTERM, array($this, "signal"));
            if(!$res){
                exit();
            }
        }
    }

    /**
     * signal handle function, need to be public
     */
    public function signal($signo) {
        static $term_count = 0;
        if(!$this->isparent){
            $this->stop_work = true;
        } else {
            switch ($signo) {
                case SIGINT:
                case SIGTERM:
                    $this->log("Shutting down...");
                    $this->stop_work = true;
                    $term_count++;
                    if($term_count < 5){
                        $this->stop_children();
                    } else {
                        $this->stop_children(SIGKILL);
                    }
                    break;
                case SIGHUP:
                    $this->log("Restarting children", self::LOG_LEVEL_PROC_INFO);
                    if ($this->log_file) {
                        $this->open_log_file($this->log_file);
                    }
                    $this->stop_children();
                    break;
                default:
                // handle all other signals
                    break;
            }
        }
    }

	/**
	 * Log the message to file
	 * @param $message
	 * @param int $level
	 */
    protected function log($message, $level=self::LOG_LEVEL_INFO) {
        static $init = false;
        if($level > $this->verbose) return;
        if(!$init){
            $init = true;
            if($this->log_file_handle){
                list($ts, $ms) = explode(".", sprintf("%f", microtime(true)));
                $ds = date("Y-m-d H:i:s").".".str_pad($ms, 6, 0);
                fwrite($this->log_file_handle, "Date                         PID   Type   Message\n");
            } else {
                echo "PID   Type   Message\n";
            }
        }
        $label = "";
        switch($level) {
            case self::LOG_LEVEL_INFO;
                $label = "INFO  ";
                break;
            case self::LOG_LEVEL_PROC_INFO:
                $label = "PROC  ";
                break;
            case self::LOG_LEVEL_WORKER_INFO:
                $label = "WORKER";
                break;
            case self::LOG_LEVEL_DEBUG:
                $label = "DEBUG ";
                break;
            case self::LOG_LEVEL_CRAZY:
                $label = "CRAZY ";
                break;
        }
        $log_pid = str_pad($this->pid, 5, " ", STR_PAD_LEFT);
        if($this->log_file_handle){
            list($ts, $ms) = explode(".", sprintf("%f", microtime(true)));
            $ds = date("Y-m-d H:i:s").".".str_pad($ms, 6, 0);
            $prefix = "[$ds] $log_pid $label";
            fwrite($this->log_file_handle, $prefix." ".str_replace("\n", "\n$prefix ", trim($message))."\n");
        } else {
            $prefix = "$log_pid $label";
            echo $prefix." ".str_replace("\n", "\n$prefix ", trim($message))."\n";
        }
    }

	/**
	 * show help message to console
	 * @param string $msg
	 */
    protected function show_help($msg = "") {
        if($msg){
            echo "ERROR:\n";
            echo "  ".wordwrap($msg, 72, "\n  ")."\n";
            exit();
        }
        echo "Job Manager Script\n\n";
        echo "USAGE:\n";
        echo "  # ".basename(__FILE__)." -h | -c CONFIG [-v] [-l LOG_FILE] [-d] [-v] [-a] [-P PID_FILE]\n\n";
        echo "OPTIONS:\n";
        echo "  -a             Automatically check for new worker code\n";
        echo "  -c CONFIG      Worker configuration file, defaults to ./config.ini\n";
        echo "  -d             Daemon, detach and run in the background\n";
        echo "  -D NUMBER      Start NUMBER workers for every job\n";
        echo "  -h HOST[:PORT] Connect to HOST and optional PORT\n";
        echo "  -H             Shows this help\n";
        echo "  -l LOG_FILE    Log output to LOG_FILE\n";
        echo "  -P PID_FILE    File to write process ID out to\n";
        echo "  -v             Increase verbosity level by one\n";
        echo "  -w DIR         Directory where workers are located, defaults to ./workers\n";
        echo "  -r NUMBER      Maximum job iterations per worker\n";
        echo "  -x SECONDS     Maximum seconds for a worker to live\n";
        echo "  -Z             Parse the command line and config file then dump it to the screen and exit.\n";
        echo "\n";
        exit();
    }

}