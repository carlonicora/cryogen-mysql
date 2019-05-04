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
 * @package carlonicora\cryogen
 * @author Carlo Nicora
 */
namespace carlonicora\cryogen\mysqlcryogen;

use carlonicora\cryogen\connectionBuilder;

/**
 * Class mySqlConnectionBuilder
 *
 * @package carlonicora\cryogen\mysqlcryogen
 */
class mySqlConnectionBuilder extends connectionBuilder{
    /**
     * @var string
     */
    public $host;

    /**
     * @var string
     */
    public $user;

    /**
     * @var string
     */
    public $password;

    /**
     * @var string
     */
    public $databaseName;

    /**
     * Initialises the connection parameters in the database-type-specific connection builder
     *
     * @param array $connectionValues
     */
    public function initialise($connectionValues){
        $this->databaseType = 'mySql';

        $this->host = $connectionValues['host'];
        $this->user = $connectionValues['user'];
        $this->password = $connectionValues['password'];
        $this->databaseName = $connectionValues['databasename'];
    }

    /**
     * Extends the database name of the connection builder
     *
     * @param string $databaseName
     */
    public function extendDatabaseName($databaseName){
        $this->databaseName = $this->databaseName . $databaseName;
    }
}