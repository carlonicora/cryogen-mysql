<?php /** @noinspection PhpUnhandledExceptionInspection */

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

use carlonicora\cryogen\cryogenException;
use carlonicora\cryogen\entity;
use carlonicora\cryogen\entityList;
use carlonicora\cryogen\SQL\sqlCryogen;
use carlonicora\cryogen\queryEngine;
use carlonicora\cryogen\metaTable;
use mysqli_stmt;
use Exception;

/**
 * Main class for the MySql plugin for cryogen
 *
 * @package carlonicora\cryogen\mysqlcryogen
 */
class mySqlCryogen extends sqlCryogen{
    /**
     * Initialises cryogen for MySql
     *
     * @param mySqlConnectionBuilder $connection
     */
    public function __construct($connection){
        $returnValue = false;

        $this->connectionController = new mySqlConnectionController();

        if ($this->connectionController->initialize($connection)){
            $this->structureController = new mySqlStructureController($this->connectionController, $this);
            $returnValue = true;
        }

        return($returnValue);
    }

    /**
     * Returns if a database exists
     *
     * @param string $databaseName
     * @return bool
     */
    public function databaseExists($databaseName){
        $returnValue = $this->connectionController->connection->select_db($databaseName);
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

    protected function setActionTransaction($sqlStatement, $sqlParameters, $isDelete=false, &$generatedId = false){
        /**
         * @var mysqli_stmt $statement
         */
        $returnValue = false;

        if ($statement = $this->connectionController->connection->prepare($sqlStatement)) {
            $this->bindParameters($statement, $sqlParameters);

            try {
                $statement->execute();
                $returnValue =  $statement->affected_rows > 0 || $this->connectionController->connection->sqlstate == '00000';
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
                        $exception = new cryogenException(cryogenException::ERROR_RUNNING_UPDATE_QUERY, 'error: ' . $this->connectionController->connection->error.'running sql:'.$sqlStatement.' with '.json_encode($sqlParameters));
                        $exception->log();
                    } else {
                        $exception = new cryogenException(cryogenException::ERROR_RUNNING_UPDATE_QUERY, 'Generic Error running: '.$sqlStatement.' with '.json_encode($sqlParameters));
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
            $this->connectionController->connection->commit();
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

            $statement->bind_result($returnValue);

            $statement->fetch();

            $statement->close();
        } else {
            throw new cryogenException(cryogenException::ERROR_PREPARING_SQL_STATEMENT, 'error: '.$this->connectionController->connection->error.'-running sql:'.$sqlStatement.'-with parameters:'.json_encode($sqlParameters));
        }

        $this->connectionController->connection->commit();

        return($returnValue);
    }

    /**
     * Bind the parameters to the mysqli_stmt before preparing the SQL
     *
     * @param $statement
     * @param $parameters
     */
    private function bindParameters(&$statement, $parameters) {
        if (isset($parameters) && sizeof($parameters) > 1 && sizeof($parameters[1])) {
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
