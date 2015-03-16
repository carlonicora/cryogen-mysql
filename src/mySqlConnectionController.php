<?php
/**
 * Copyright 2015 Carlo Nicora
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * @license Apache
 * @license http://www.apache.org/licenses/LICENSE-2.0
 * @package CarloNicora\cryogen
 * @author Carlo Nicora
 */

namespace CarloNicora\cryogen\mySqlCryogen;

use CarloNicora\cryogen\connectionController;
use mysqli;

class mySqlConnectionController extends connectionController{

    /**
     * @var mysqli
     */
    public $connection;

    /**
     * Stores the connection details in the connection controller and opens the connection to the database
     *
     * @param array $connectionString
     * @return bool
     */
    public function initialize($connectionString){
        $this->connectionString = $connectionString;

        $returnValue = $this->connect();

        $this->isConnected = $returnValue;

        return($returnValue);
    }

    public function connect(){
        $returnValue = TRUE;

        if (!isset($this->connection)) {

            if (isset($this->connectionString[3])){
                @$this->connection = new \mysqli($this->connectionString[0], $this->connectionString[1], $this->connectionString[2], $this->connectionString[3]);
            } else {
                @$this->connection = new \mysqli($this->connectionString[0], $this->connectionString[1], $this->connectionString[2]);
            }

        } else {
            @$thread = $this->connection->thread_id;
            if (!isset($thread)) {
                if (isset($this->connectionString[3])) {
                    @$this->connection->connect($this->connectionString[0], $this->connectionString[1], $this->connectionString[2], $this->connectionString[3]);
                } else {
                    @$this->connection->connect($this->connectionString[0], $this->connectionString[1], $this->connectionString[2]);
                }
            }
            unset($thread);
        }

        if (mysqli_connect_errno()) {
            $this->cryogen->log("Cannot connect to the database: " . mysqli_connect_error(), E_USER_ERROR, TRUE);
            $returnValue = FALSE;
        } else {
            $this->connection->autocommit(FALSE);
        }

        return($returnValue);
    }

    public function disconnect(){
        if(isset($this->connection) && isset($this->connection->thread_id)){
            $this->connection->close();
        }
    }

    public function getDatabaseName(){
        return($this->connectionString[3]);
    }

    public function createDatabase($databaseName){
        if ($this->connection->query("CREATE DATABASE " . $databaseName) === TRUE) {
            $this->connection->select_db($databaseName);
        }
    }
}
?>