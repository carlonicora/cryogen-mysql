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

use CarloNicora\cryogen\structureController;
use CarloNicora\cryogen\metaField;
use CarloNicora\cryogen\metaTable;

class mySqlStructureController extends structureController{
    /** @var  $connectionController mySqlConnectionController */
    protected $connectionController;
    protected $cryogen;



    public function __construct($connectionController, $cryogen){
        $this->connectionController = $connectionController;
        $this->cryogen = $cryogen;
    }

    public function createView($viewSql){
        $this->connectionController->connection->query($viewSql);
    }

    /**
     * @param $metaTable metaTable
     * @param bool $isFederated
     * @param null $federatedLink
     * @return bool|void
     */
    public function createTable($metaTable, $isFederated = FALSE, $federatedLink = NULL){

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

        $this->connectionController->connection->query($sqlQuery);
    }

    /**
     * @param metaTable $metaTable
     * @return bool
     */
    public function updateTable($metaTable){
        /**
         * @var bool $returnValue
         * @var metaTable $dbTable
         * @var metaField $metaField
         * @var metaField $dbField
         */
        $returnValue = false;

        $dbTable = $this->readTableStructure($metaTable->name);

        foreach ($metaTable->fields as $metaField){
            $dbField = $dbTable->fields->getFieldByName($metaField->name);
        }

        return($returnValue);
    }

    public function readStructure(){
        $returnValue = [];

        $this->connectionController->connect();

        $tableName = null;

        $sqlStatement = "SHOW TABLES FROM " . $this->connectionController->getDatabaseName();
        if ($stmt = $this->connectionController->connection->prepare($sqlStatement)) {
            $stmt->execute();
            $stmt->bind_result($tableName);
            while ($stmt->fetch()){
                $metaTable = new metaTable($tableName, $tableName);
                $returnValue[] = $metaTable;
                $metaTable = NULL;
            }
            $stmt->close();
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
        $returnValue = new metaTable($tableName, $tableName);

        $sqlStatement = "DESCRIBE " . $tableName;

        $name = $type = $null = $key = $default = $extra = null;

        if ($stmt = $this->connectionController->connection->prepare($sqlStatement)) {
            $stmt->execute();
            $stmt->bind_result($name, $type, $null, $key, $default, $extra);
            $position = 0;
            while ($stmt->fetch()){
                $size=stristr($type, "(");
                $type = substr($type, 0, strlen($type) - strlen($size));
                $size = substr($size, 1, strlen($size)-2);
                $returnValue->fields[] = new metaField($position, $name, $type, $size, $key=="PRI", $extra=="auto_increment");
                $position++;
            }
            $stmt->close();
        }

        return($returnValue);
    }
}
?>