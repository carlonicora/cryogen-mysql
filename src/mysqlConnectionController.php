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
 * @package carlonicora\cryogen\mysqlcryogen
 * @author Carlo Nicora
 */

namespace carlonicora\cryogen\mysqlcryogen;

use carlonicora\cryogen\connectionController;
use carlonicora\cryogen\cryogenException;
use mysqli;

/**
 * Implementation of the connection controller for MySql
 *
 * @package carlonicora\cryogen\mysqlcryogen
 */
class mysqlConnectionController extends connectionController{
    /**
     * @var mysqlConnectionBuilder $connmysqlcryogenectionValues
     */
    public $connectionValues;

    /**
     * Opens a connection to the database
     *
     * @return bool
     */
    public function connect(){
        $returnValue = true;

        if (!isset($this->connection)) {
            if (isset($this->connectionValues->databaseName)){
                @$this->connection = new mysqli($this->connectionValues->host, $this->connectionValues->user, $this->connectionValues->password, $this->connectionValues->databaseName);
            } else {
                @$this->connection = new mysqli($this->connectionValues->host, $this->connectionValues->user, $this->connectionValues->password);
            }
        } else {
            if (!isset($this->connection->thread_id)) {
                if (isset($this->connectionValues->databaseName)) {
                    @$this->connection->connect($this->connectionValues->host, $this->connectionValues->user, $this->connectionValues->password, $this->connectionValues->databaseName);
                } else {
                    @$this->connection->connect($this->connectionValues->host, $this->connectionValues->user, $this->connectionValues->password);
                }
            }
        }

        $this->connection->set_charset("utf8");

        if ($this->connection->connect_error) {
            $exception = new cryogenException(cryogenException::FAILURE_CREATING_DATABASE_CONNECTION, 'Connect Error: '. $this->connection->connect_errno.'-'.$this->connection->connect_error);
            $exception->log();
            $returnValue = false;
        } else {
            $this->connection->autocommit(false);
        }

        return($returnValue);
    }

    /**
     * Closes a connection to the database
     *
     * @return bool
     */
    public function disconnect(){
        if($this->isConnected()){
            $this->connection->close();
        }

        return(true);
    }

    /**
     * Returns the name of the database specified in the connection
     *
     * @return string
     */
    public function getDatabaseName(){
        return($this->connectionValues->databaseName);
    }

    /**
     * Create a new Database
     *
     * @param string $databaseName
     * @return bool
     */
    public function createDatabase($databaseName){
        $returnValue = false;

        if($this->isConnected()){
            if ($this->connection->query("CREATE DATABASE " . $databaseName) === true) {
                $this->connection->select_db($databaseName);
                $returnValue = true;
            }
        }

        return($returnValue);
    }

    /**
     * Identifies if there is an active connection to the database
     *
     * @return bool
     */
    public function isConnected(){
        return(isset($this->connection) && isset($this->connection->thread_id));
    }
}
