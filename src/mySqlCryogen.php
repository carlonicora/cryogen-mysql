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
 * @author Carlo Nicora
 */
namespace CarloNicora\cryogen\mySqlCryogen;

use CarloNicora\cryogen\cryogen;

use CarloNicora\cryogen\cryogenException;
use CarloNicora\cryogen\entity;
use CarloNicora\cryogen\entityList;
use CarloNicora\cryogen\queryEngine;
use CarloNicora\cryogen\metaTable;
use CarloNicora\cryogen\metaField;
use mysqli_stmt;
use Exception;

/**
 * Main class for the MySql plugin for cryogen
 *
 * @package CarloNicora\cryogen\mySqlCryogen
 */
class mySqlCryogen extends cryogen{
    /**
     * Initialises cryogen for MySql
     *
     * @param array $connectionString
     */
    public function __construct($connectionString){
        $returnValue = false;

        $this->connectionController = new mySqlConnectionController();

        if ($this->connectionController->initialize($connectionString)){
            $this->structureController = new mySqlStructureController($this->connectionController, $this);
            $returnValue = true;
        }

        return($returnValue);
    }

    /**
     * @param metaTable|null $meta
     * @param entity|null $entity
     * @param null $valueOfKeyField
     * @return mixed
     */
    public function generateQueryEngine(metaTable $meta=null, entity $entity=null, $valueOfKeyField=null){
        $returnValue = new mySqlQueryEngine($meta, $entity, $valueOfKeyField);

        return($returnValue);
    }

    /**
     * Clears the resources
     */
    public function __destruct(){
        if (isset($this->connectionController) && $this->connectionController->isConnected()){
            $this->connectionController->disconnect();
        } else if (isset($this->connectionController)){
            unset($this->connectionController);
        }
    }

    /**
     * Updates an entity in the database.
     *
     * If the entity is not existing in the database, cryogen performs an INSERT, otherwise an UPDATE
     *
     * @param entity $entity
     * @return bool
     */
    public function update(entity $entity){
        /**
         * @var entity $entity
         * @var queryEngine $engine
         * @var bool $returnValue
         * @var bool $noEntitiesModified
         * @var array $sqlParameters;
         */
        $returnValue = true;

        $noEntitiesModified = true;

        if (isset($entity) && gettype($entity) != "array" &&  $entity->isEntityList) {
            $entityList = $entity;
        } else {
            if (gettype($entity) != "array"){
                $entityList = [];
                $entityList[] = $entity;
            } else {
                $entityList = $entity;
            }
        }

        $this->connectionController->connect();

        foreach ($entityList as $entity){
            if ($entity->status() != entity::ENTITY_NOT_MODIFIED){
                $noEntitiesModified = false;

                $engine = $this->generateQueryEngine(NULL, $entity);

                try{
                    $sqlStatement = $entity->status() == entity::ENTITY_NOT_RETRIEVED ? $engine->generateInsertStatement() : $engine->generateUpdateStatement();
                    $sqlParameters = $entity->status() == entity::ENTITY_NOT_RETRIEVED ? $engine->generateInsertParameters() : $engine->generateUpdateParameters();

                    if (entity::ENTITY_NOT_RETRIEVED && $engine->hasAutoIncrementKey()){
                        $keyField = $engine->getAutoIncrementKeyName();
                        $entity->$keyField = true;
                        $returnValue = $this->setActionTransaction($sqlStatement, $sqlParameters, false, $entity->$keyField);
                    } else {
                        $returnValue = $this->setActionTransaction($sqlStatement, $sqlParameters);
                    }
                } catch (cryogenException $exception){
                    $exception->log();
                    $returnValue = null;
                }

                if (!$returnValue){
                    break;
                }
            }
        }

        if (!$noEntitiesModified){
            if ($returnValue){
                $returnValue = $this->completeActionTransaction($returnValue);

                if ($returnValue){
                    foreach ($entityList as $entity){
                        $entity->setInitialValues();
                        $entity->setRetrieved();
                    }
                }

            } else {
                $this->completeActionTransaction($returnValue);
            }
        }

        return($returnValue);
    }

    /**
     * Deletes an entity in the database.
     *
     * @param entity $entity
     * @param queryEngine|null $engine
     * @return bool
     */
    public function delete(entity $entity=null, queryEngine $engine=null){
        /**
         * @var array $sqlParameters;
         */
        $returnValue = false;

        if (!isset($entity) && !isset($engine)){
            $exception = new cryogenException(cryogenException::EMPTY_DELETE_PARAMETERS);
            $exception->log();
        } else {
            $entityList = null;

            if (isset($entity)) {
                if (isset($entity) && gettype($entity) != "array" && $entity->isEntityList) {
                    $entityList = $entity;
                } else {
                    if (isset($entity)) {
                        if (gettype($entity) != "array") {
                            $entityList = [];
                            $entityList[] = $entity;
                        } else {
                            $entityList = $entity;
                        }
                    }
                }
            }

            $this->connectionController->connect();

            if (isset($entityList)) {
                foreach ($entityList as $entity) {
                    if (isset($entity)) {
                        $engine = $this->generateQueryEngine(null, $entity);
                    }

                    try{
                        $sqlStatement = $engine->generateDeleteStatement();
                        $sqlParameters = $engine->generateDeleteParameters();

                        $returnValue = $this->setActionTransaction($sqlStatement, $sqlParameters, true);
                    } catch (cryogenException $exception){
                        $exception->log();
                        $returnValue = null;
                    }

                    if (!$returnValue) {
                        break;
                    }
                }
            } else {
                try{
                    $sqlStatement = $engine->generateDeleteStatement();
                    $sqlParameters = $engine->generateDeleteParameters();

                    $returnValue = $this->setActionTransaction($sqlStatement, $sqlParameters, true);
                } catch (cryogenException $exception){
                    $exception->log();
                    $returnValue = null;
                }
            }

            if ($returnValue) {
                $returnValue = $this->completeActionTransaction($returnValue);
            } else {
                $this->completeActionTransaction($returnValue);
            }
        }

        return($returnValue);
    }

    /**
     * Reads a list of records identified by the query engine.
     *
     * If the levels of relations to load is > 0, then cryogen will load records related to a single foreign key as
     * defined in the database objects
     *
     * @param queryEngine $engine
     * @param int $levelsOfRelationsToLoad
     * @param metaTable|null $metaTableCaller
     * @param metaField|null $metaFieldCaller
     * @param bool $isSingle
     * @return entity|entityList|null
     */
    public function read(queryEngine $engine, $levelsOfRelationsToLoad=0, metaTable $metaTableCaller=null, metaField $metaFieldCaller=null, $isSingle=false){
        /**
         * @var array $sqlParameters;
         */
        $this->connectionController->connect();

        try{
            $sqlStatement = $engine->generateReadStatement();
            $sqlParameters = $engine->generateReadParameters();

            $returnValue = $this->setReadTransaction($engine, $sqlStatement, $sqlParameters);
        } catch (cryogenException $exception){
            $exception->log();
            $returnValue = null;
        }

        if ($levelsOfRelationsToLoad > 0 && ($returnValue && sizeof($returnValue) > 0)){
            if (isset($engine->meta->relations) && sizeof($engine->meta->relations) > 0){
                foreach ($engine->meta->relations as $relation){
                    $relationTarget = $relation->target;
                    if ((!isset($metaFieldCaller) && !isset($metaTableCaller)) || ($metaFieldCaller != $relation->linkedField || $metaTableCaller != $relation->linkedTable)){
                        $engine = NULL;
                        eval("\$engine = \$this->generateQueryEngine(" . $relation->linkedTable . "::\$table);");
                        foreach($returnValue as $parentEntity){
                            $fieldName = '';
                            eval("\$fieldName = " . $relation->table . "::\$" . $relation->field . "->name;");
                            $keyValue = $parentEntity->$fieldName;
                            eval("\$engine->setDiscriminant(" . $relation->linkedTable . "::\$" . $relation->linkedField . ", '" . $keyValue . "', \"=\", \" OR \");");
                        }
                        if ($levelsOfRelationsToLoad > 1){
                            $childrenEntities = $this->read($engine, $levelsOfRelationsToLoad-1, $relation->table, $relation->field);
                        } else {
                            $childrenEntities = $this->read($engine, $levelsOfRelationsToLoad-1);
                        }
                        $engine = null;

                        $parentFieldName = '';
                        $childFieldName = '';
                        eval("\$parentFieldName = " . $relation->table . "::\$" . $relation->field . "->name;");
                        eval("\$childFieldName = " . $relation->linkedTable . "::\$" . $relation->linkedField . "->name;");

                        foreach($returnValue as $parentEntity){
                            foreach ($childrenEntities as $childEntity){
                                $isFine = $parentEntity->$parentFieldName == $childEntity->$childFieldName;
                                if ($isFine){
                                    if ($relation->relationType == 0){
                                        $parentEntity->$relationTarget = $childEntity;
                                        break;
                                    } else {
                                        $parentEntity->{$relationTarget}[] = $childEntity;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        if ($isSingle){
            if (isset($returnValue) && sizeof($returnValue)==1){
                $returnValue = $returnValue[0];
            } else {
                $returnValue = null;
            }
        } else {
            if (sizeof($returnValue) > 0){
                $returnValue->meta = $engine->meta;
            } else {
                $returnValue = NULL;
            }
        }

        return($returnValue);
    }

    /**
     * Reads one single record identified by the query engine.
     *
     * If the query returns more than one record, the system generates an error. This function is designed to return
     * a single-record query, not the first of many records.
     * If the levels of relations to load is > 0, then cryogen will load records related to a single foreign key as
     * defined in the database objects
     *
     * @param queryEngine $engine
     * @param int $levelsOfRelationsToLoad
     * @param metaTable|null $metaTableCaller
     * @param metaField|null $metaFieldCaller
     * @return entityList
     */
    public function readSingle(queryEngine $engine, $levelsOfRelationsToLoad=0, metaTable $metaTableCaller=null, metaField $metaFieldCaller=null){
        return($this->read($engine, $levelsOfRelationsToLoad, $metaTableCaller, $metaFieldCaller, true));
    }

    /**
     * Returns the number of records matching the query in the query engine
     *
     * @param queryEngine $engine
     * @return int
     */
    public function count(queryEngine $engine){
        /**
         * @var array $sqlParameters;
         */
        $returnValue = 0;

        $this->connectionController->connect();

        try{
            $sqlStatement = $engine->generateReadCountStatement();
            $sqlParameters = $engine->generateReadParameters();

            $returnValue = $this->setCountTransaction($engine, $sqlStatement, $sqlParameters);
        } catch (cryogenException $exception){
            $exception->log();
        }

        return($returnValue);
    }

    /**
     * Runs the transactional INSERT, UPDATE or DELETE query on the database
     *
     * @param string $sqlStatement
     * @param array $sqlParameters
     * @param bool $isDelete
     * @param bool $generatedId
     * @return entityList
     * @throws cryogenException
     */
    protected function setActionTransaction($sqlStatement, $sqlParameters, $isDelete=false, &$generatedId = false){
        /**
         * @var mysqli_stmt $statement
         */
        $returnValue = false;

        if ($statement = $this->connectionController->connection->prepare($sqlStatement)) {
            $this->bindParameters($statement, $sqlParameters);

            try {
                $statement->execute();
                $returnValue =  $statement->affected_rows > 0;
            } catch (Exception $systemException){
                $exception = new cryogenException(cryogenException::ERROR_RUNNING_UPDATE_QUERY, 'error: '.$this->connectionController->connection->error);
                $exception->log();
            }

            if ($returnValue){
                if ($generatedId){
                    $generatedId = $statement->insert_id;
                }
            } else {
                if (!$isDelete) {
                    if (isset($this->connectionController->connection->error) && $this->connectionController->connection->error != '') {
                        $exception = new cryogenException(cryogenException::ERROR_RUNNING_UPDATE_QUERY, 'error: ' . $this->connectionController->connection->error);
                        $exception->log();
                    }
                }
            }

            $statement->close();
        } else {
            throw new cryogenException(cryogenException::ERROR_PREPARING_SQL_STATEMENT, 'error: '.$this->connectionController->connection->error.'-running sql:'.$sqlStatement.'-with parameters:'.json_encode($sqlParameters));
        }

        return($returnValue);
    }

    /**
     * Commit the INSERT, UPDATE or DELETE transaction on the database
     *
     * @param bool $commit
     * @return bool
     */
    protected function completeActionTransaction($commit){
        /**
         * @var bool $returnValue
         */
        $returnValue = false;

        if ($commit){
            $returnValue = $this->connectionController->connection->commit();

            if (!$returnValue){
                if ($this->connectionController->connection->errno == 0){
                    $returnValue = true;
                } else {
                    $exception = new cryogenException(cryogenException::ERROR_COMMITTING_QUERY, 'error: '.$this->connectionController->connection->error);
                    $exception->log();
                }
            }
        } else {
            $this->connectionController->connection->rollback();
        }

        return($returnValue);
    }

    /**
     * Runs the transactional SELECT query on the database
     *
     * @param queryEngine $engine
     * @param string $sqlStatement
     * @param array $sqlParameters
     * @return entityList
     * @throws cryogenException
     */
    protected function setReadTransaction(queryEngine $engine, $sqlStatement, $sqlParameters){
        /**
         * @var mysqli_stmt $statement
         * @var entity $record
         */
        $returnValue = new entityList();

        if ($statement = $this->connectionController->connection->prepare($sqlStatement)) {
            $this->bindParameters($statement, $sqlParameters);

            $statement->execute();

            $statement->store_result();

            if ($statement->num_rows>0) {
                $selectedFields = $engine->getFieldsVariables();
                $selectedDynamicFields = $engine->getDynamicFieldsVariables();
                $selectedFields = array_merge($selectedFields, $selectedDynamicFields);

                if (sizeof($selectedFields) == $statement->result_metadata()->field_count) {
                    $fields = "";
                    foreach ($selectedFields as $fieldName) {
                        $fields .= "\$" . $fieldName . ", ";
                    }
                    $fields = substr($fields, 0, strlen($fields) - 2);

                    eval("\$statement->bind_result($fields);");

                    while ($statement->fetch()) {
                        $record = new $engine->meta->object;

                        $record->entityRetrieved = TRUE;

                        foreach ($selectedFields as $selectedFieldName) {
                            $record->$selectedFieldName = ${$selectedFieldName};
                        }
                        $record->setInitialValues();
                        $record->setRetrieved();

                        $returnValue[] = $record;
                        unset($record);

                        if (!isset($returnValue->meta)){
                            $returnValue->meta = $engine->meta;
                        }
                    }

                    $statement->close();
                    unset($statement);
                } else {
                    throw new cryogenException(cryogenException::ERROR_BINDING_OBJECT_TO_TABLE_PARAMETERS, 'database table: '.$engine->meta->name.' - database object: '.$engine->meta->object);
                }

            }
        } else {
            throw new cryogenException(cryogenException::ERROR_PREPARING_SQL_STATEMENT, 'error: '.$this->connectionController->connection->error.'-running sql:'.$sqlStatement.'-with parameters:'.json_encode($sqlParameters));
        }

        return($returnValue);
    }

    /**
     * Specialised transaction that counts the records matching a specific query engine on the database
     *
     * @param queryEngine $engine
     * @param string $sqlStatement
     * @param array $sqlParameters
     * @return int
     * @throws cryogenException
     */
    protected function setCountTransaction(queryEngine $engine, $sqlStatement, $sqlParameters){
        /**
         * @var mysqli_stmt $statement
         */
        if ($statement = $this->connectionController->connection->prepare($sqlStatement)) {
            $this->bindParameters($statement, $sqlParameters);

            $statement->execute();
            $statement->store_result();

            $returnValue = $statement->num_rows;

            $statement->close();
        } else {
            throw new cryogenException(cryogenException::ERROR_PREPARING_SQL_STATEMENT, 'error: '.$this->connectionController->connection->error.'-running sql:'.$sqlStatement.'-with parameters:'.json_encode($sqlParameters));
        }

        return($returnValue);
    }

    /**
     * Bind the parameters to the mysqli_stmt before preparing the SQL
     *
     * @param $statement
     * @param $parameters
     */
    private function bindParameters(&$statement, $parameters) {
        if (isset($parameters) && sizeof($parameters) > 0) {
            $paramString = array_shift($parameters);

            $bindParams[] = $paramString;

            foreach ($parameters[0] as $value) {
                $bindParams[] = $value;
            }

            call_user_func_array([$statement, 'bind_param'], $this->refValues($bindParams));
        }
    }

    /**
     * @param $arr
     * @return array
     */
    private function refValues($arr) {
        $refs = [];

        foreach ($arr as $key => $value) {
            $refs[$key] = &$arr[$key];
        }

        return $refs;
    }
}
?>