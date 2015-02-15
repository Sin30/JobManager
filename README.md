# JobManager

This is a simple php job manager for sending heavy jobs to backend workers, so that the requests can by answered very fast.

There are two key components of this system.

* **JobManager** (a process manager for workers, who control the spawn of the actual workers)
* **JobRedis** (a redis job queue manager, who provides methods to push the job to the queue)

## Basic Usage
1. Create a function to do the job

	Here is an example by creating "reverse string" job in workers/reverse.php
		
		<?php
		function reverse($string) {
    		return strrev($string);
		}

2. Start the job manager

	run the job manager by command
		
		./run.sh start

3. Send the job to the queue

	In your request handling code, do the following
	
		$jobRedis = new JobRedis('127.0.0.1', 6379);
		$load = 'Hello World';
		$jobRedis->sendJob('reverse', $load);
		//or you can choice to get the result right away by doing
		$result = $jobRedis->sendJob('reverse', $load, true);
		
## Config


## Mangement