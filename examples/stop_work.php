<?php

require "include.php";

//stop all the workers
$jobRedis->sendOrder([
	'name' => 'stop_work',
]);