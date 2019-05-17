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
 * @package carlonicora\cryogen\mysqlcryogen;
 * @author Carlo Nicora
 */

namespace carlonicora\cryogen\mysqlcryogen;

use carlonicora\cryogen\structureController;
use carlonicora\cryogen\metaField;
use carlonicora\cryogen\metaTable;
use mysqli_stmt;

/**
 * Implements the cryogen structure controller for MySql
 *
 */
class mysqlStructureController extends structureController{
    /**
     * @var mysqlConnectionController $connectionController
     */
    protected $connectionController;

    /**
     * @var mysqlCryogen cryogen
     */
    protected $cryogen;

    /**
     * Initialises the structure controller class
     *
     * @param mysqlConnectionController $connectionController
     * @param mysqlCryogen $cryogen
     */
    public function __construct(mysqlConnectionController $connectionController, mysqlCryogen $cryogen){
        $this->connectionController = $connectionController;
        $this->cryogen = $cryogen;
    }

    /**
     * Returns the structure of all the tables in the connected database
     *
     * @return array
     */
    public function readStructure(){
        /**
         * @var mysqli_stmt $statement
         */
        $returnValue = [];

        $this->connectionController->connect();

        $tableName = null;

        $sqlStatement = "SHOW TABLES FROM " . $this->connectionController->getDatabaseName();
        if ($statement = $this->connectionController->connection->prepare($sqlStatement)) {
            $statement->execute();
            $statement->bind_result($tableName);
            while ($statement->fetch()){
                $metaTable = new metaTable($tableName, $tableName);
                $returnValue[] = $metaTable;
                $metaTable = NULL;
            }
            $statement->close();
        }

        /** @var metaTable $metaTable */
        foreach ($returnValue as $metaTable){
            /** @var metaTable $readMetaTable */
            $readMetaTable = $this->readTableStructure($metaTable->name);

            $metaTable->fields = $readMetaTable->fields;
        }
        return($returnValue);
    }

    /**
     * Read the structure of a table from the database and returns the metaTable object
     *
     * @param $tableName
     * @return metaTable
     */
    public function readTableStructure($tableName){
        /**
         * @var mysqli_stmt $statement
         */
        $returnValue = new metaTable($tableName, $tableName);

        $sqlStatement = "DESCRIBE " . $tableName;

        $name = $type = $null = $key = $default = $extra = null;

        if ($statement = $this->connectionController->connection->prepare($sqlStatement)) {
            $statement->execute();
            $statement->bind_result($name, $type, $null, $key, $default, $extra);
            $position = 0;
            while ($statement->fetch()){
                $size=stristr($type, "(");
                $type = substr($type, 0, strlen($type) - strlen($size));
                $size = substr($size, 1, strlen($size)-2);
                $returnValue->fields[] = new metaField($position, $name, $type, $size, $key=="PRI", $extra=="auto_increment");
                $position++;
            }
            $statement->close();
        }

        return($returnValue);
    }

    /**
     * Creates a view based on the specified sql code
     *
     * @param $viewSql
     * @return bool
     */
    public function createView($viewSql){
        return($this->connectionController->connection->query($viewSql));
    }

    /**
     * Creates a table on the database using the meta table passed as parameter
     *
     * @param metaTable $metaTable
     * @param bool $isFederated
     * @param string $federatedLink
     * @return bool
     */
    public function createTable(metaTable $metaTable, $isFederated=false, $federatedLink=null){
        $sqlQuery = "CREATE TABLE " . $metaTable->name . "(";

        foreach ($metaTable->fields as $metaField){
            /** @var $metaField metaField */
            $sqlQuery .= $metaField->name . " " . $metaField->type;
            if (!is_null($metaField->size)){
                $sqlQuery .= "(" . $metaField->size . ")";
            }
            if ($metaField->isAutoNumbering == TRUE){
                $sqlQuery .= " AUTO_INCREMENT";
            }
            $sqlQuery .= ",";
        }

        $keyFields = $metaTable->getKeyFields();
        if (count($keyFields) > 0){
            $sqlQuery .= "PRIMARY KEY (";
            foreach ($keyFields as $metaField ){
                $sqlQuery .= $metaField->name . ", ";
            }
            $sqlQuery = substr($sqlQuery, 0, -2);
            $sqlQuery .= "),";
        }

        $sqlQuery = substr($sqlQuery, 0, -1);

        $sqlQuery .= ")";

        if ($isFederated){
            $sqlQuery .= "ENGINE=FEDERATED CONNECTION='" . $federatedLink . "';";
        }
        $sqlQuery .= ";";

        $returnValue = $this->connectionController->connection->query($sqlQuery);

        return($returnValue);
    }

    /**
     * Updates a table on the database using the meta table passed as parameter
     *
     * @param metaTable $metaTable
     * @return bool
     *
     * @todo Implement the method
     */
    public function updateTable(metaTable $metaTable){
        $returnValue = false;

        return($returnValue);
    }

    /**
     * Drops a table from the database using the meta table passed as parameter
     *
     * @param metaTable $metaTable
     * @return bool
     */
    public function dropTable(metaTable $metaTable){
        $sqlQuery = 'DROP TABLE ' . $metaTable->name . ';';

        $returnValue = $this->connectionController->connection->query($sqlQuery);

        return($returnValue);
    }

    /**
     * Truncates a table on the database using the meta table passed as parameter
     *
     * @param metaTable $metaTable
     * @return bool
     */
    public function truncateTable(metaTable $metaTable){
        $sqlQuery = 'TRUNCATE TABLE ' . $metaTable->name . ';';

        $returnValue = $this->connectionController->connection->query($sqlQuery);

        return($returnValue);
    }
}
