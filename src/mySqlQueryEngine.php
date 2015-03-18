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
 * @package CarloNicora\cryogen\mySqlCryogen
 * @author Carlo Nicora
 */

namespace CarloNicora\cryogen\mySqlCryogen;

use CarloNicora\cryogen\discriminant;
use CarloNicora\cryogen\queryEngine;
use CarloNicora\cryogen\metaField;

/**
 * The mySqlQueryEngine specialises the queryEngine to prepare the SQL queries specifically for MySql
 */
class mySqlQueryEngine extends queryEngine{

    /**
     * Generates the SQL statement needed for a count sql query
     *
     * @return string
     */
    public function generateReadCountStatement(){
        $returnValue = 'SELECT '.$this->getCountField().' FROM '.$this->meta->name.$this->getWhereClause().';';
        return($returnValue);
    }

    /**
     * Geneates the SQL statement needed for a read sql query
     *
     * @return string
     */
    public function generateReadStatement(){
        $returnValue = 'SELECT '.$this->getSelectedFields().$this->getDynamicFields().' FROM '.$this->meta->name.$this->getWhereClause().$this->getHavingClause().$this->getGroupingClause().$this->getOrderClause().$this->getLimitClause().';';
        return($returnValue);
    }

    /**
     * Generates an array containing the parameters needed in WHERE or HAVING clauses
     *
     * @return array
     */
    public function generateReadParameters(){
        $returnValue = [];

        if (isset($this->discriminants) && sizeof($this->discriminants) > 0){
            $returnValue[0] = '';
            $returnValue[1] = [];
            foreach ($this->discriminants as $discriminant){
                if (isset($discriminant->value)){
                    $returnValue[0] .= $this->getDiscriminantTypeCast($discriminant->metaField);
                    $returnValue[1][] = $discriminant->value;
                }
            }
        }

        if (isset($this->dynamicDiscriminant) && sizeof($this->dynamicDiscriminant) > 0){
            if (!isset($this->discriminants) || sizeof($this->discriminants) == 0) {
                $returnValue[0] = '';
                $returnValue[1] = [];
            }
            foreach ($this->dynamicDiscriminant as $discriminant){
                if (isset($discriminant->value)){
                    if (!is_object($discriminant->value)) {
                        $returnValue[0] .= $this->getDiscriminantTypeCast($discriminant->metaField);
                        $returnValue[1][] = $discriminant->value;
                    }
                }
            }
        }

        return($returnValue);
    }

    /**
     * Generates the SQL statement needed for an UPDATE sql query
     *
     * @return string
     */
    public function generateUpdateStatement(){
        $returnValue = 'UPDATE '.$this->meta->name.' SET '.$this->getNormalUpdateFields().' WHERE '.$this->getKeyUpdateFields().';';

        return($returnValue);
    }

    /**
     * Generates an array containing the parameters needed in an UPDATE sql query
     *
     * @return array
     */
    public function generateUpdateParameters(){
        $returnValue = [];

        $returnValue[0] = '';

        if (isset($this->normalFields) && sizeof($this->normalFields) > 0){
            if (!isset($returnValue[1])){
                $returnValue[1] = [];
            }
            foreach ($this->normalFields as $discriminant){
                $returnValue[0] .= $this->getDiscriminantTypeCast($discriminant->metaField);
                $returnValue[1][] = $discriminant->value;
            }
        }

        if (isset($this->keyFields) && sizeof($this->keyFields) > 0){
            if (!isset($returnValue[1])){
                $returnValue[1] = [];
            }
            foreach ($this->keyFields as $discriminant){
                $returnValue[0] .= $this->getDiscriminantTypeCast($discriminant->metaField);
                $returnValue[1][] = $discriminant->value;
            }
        }

        return($returnValue);
    }

    /**
     * Generates the SQL statement needed for an INSERT sql query
     *
     * @return string
     */
    public function generateInsertStatement(){
        $fieldParams = '';
        $returnValue = 'INSERT INTO '.$this->meta->name.' ('.$this->getInsertFields($fieldParams).') VALUES ('.$fieldParams.');';

        return($returnValue);
    }

    /**
     * Generates an array containing the parameters needed in an INSERT sql query
     *
     * @return array
     */
    public function generateInsertParameters(){
        $returnValue = [];

        $returnValue[0] = '';

        if (isset($this->keyFields) && sizeof($this->keyFields) > 0){
            if (!isset($returnValue[1])){
                $returnValue[1] = [];
            }
            foreach ($this->keyFields as $discriminant){
                if (!$discriminant->metaField->isAutoNumbering){
                    $returnValue[0] .= $this->getDiscriminantTypeCast($discriminant->metaField);
                    $returnValue[1][] = $discriminant->value;
                }
            }
        }

        if (isset($this->normalFields) && sizeof($this->normalFields) > 0){
            if (!isset($returnValue[1])){
                $returnValue[1] = [];
            }
            foreach ($this->normalFields as $discriminant){
                $returnValue[0] .= $this->getDiscriminantTypeCast($discriminant->metaField);
                $returnValue[1][] = $discriminant->value;
            }
        }

        return($returnValue);
    }

    /**
     * Generates the SQL statement needed for a DELETE sql query
     *
     * @return string
     */
    public function generateDeleteStatement(){
        $returnValue = 'DELETE FROM ' . $this->meta->name . ' WHERE ';
        $sqlGenerated = FALSE;

        $lastDiscriminant = '';

        if (isset($this->keyFields) && sizeof($this->keyFields) > 0){
            foreach($this->keyFields as $discriminant){
                if (isset($discriminant->value)){
                    $returnValue .= $discriminant->metaField->name . $discriminant->clause . '?' . $discriminant->connector;
                    $lastDiscriminant = $discriminant->connector;
                    $sqlGenerated = TRUE;
                }
            }
        }

        if (!$sqlGenerated) {
            if (isset($this->discriminants) && sizeof($this->discriminants) > 0){
                foreach($this->discriminants as $discriminant){
                    if (isset($discriminant->value)){
                        $returnValue .= $discriminant->metaField->name . $discriminant->clause . '?' . $discriminant->connector;
                        $lastDiscriminant = $discriminant->connector;
                    }
                }
            } else {
                foreach ($this->normalFields as $discriminant){
                    if (isset($discriminant->value)){
                        $returnValue .= $discriminant->metaField->name . $discriminant->clause . '?' . $discriminant->connector;
                        $lastDiscriminant = $discriminant->connector;
                    }
                }
            }
        }

        if (!$lastDiscriminant){
            $lastDiscriminant = '';
        }
        $returnValue = substr($returnValue, 0, strlen($returnValue)-strlen($lastDiscriminant));

        if (substr($returnValue, -7) == ' WHERE '){
            $returnValue = substr($returnValue, 0, -7);
        }

        return($returnValue);
    }

    /**
     * Generates an array containing the parameters needed in aDELETE sql query
     *
     * @return array
     */
    public function generateDeleteParameters(){
        $returnValue = [];

        $returnValue[0] = '';

        $sqlGenerated = FALSE;

        $keys = null;

        if (isset($this->keyFields) && sizeof($this->keyFields) > 0){
            foreach($this->keyFields as $discriminant){
                if (isset($discriminant->value)){
                    $keys = $this->keyFields;
                    $sqlGenerated = TRUE;
                }
            }

        }
        if (!$sqlGenerated) {
            if (isset($this->discriminants) && sizeof($this->discriminants) > 0){
                $keys = $this->discriminants;
            } else {
                $keys = $this->normalFields;
            }
        }

        foreach($keys as $discriminant){
            if (isset($discriminant->value)){
                $returnValue[0] .= $this->getDiscriminantTypeCast($discriminant->metaField);
                $returnValue[1][] = $discriminant->value;
            }
        }
        return($returnValue);
    }

    /**
     * Generates the part of the sql query specifying the LIMIT sql function
     *
     * @return string
     */
    private function getLimitClause(){
        $returnValue = '';

        if ($this->limitStart || $this->limitLength){
            $returnValue = ' LIMIT ' . $this->limitStart . ',' . $this->limitLength;
        }

        return($returnValue);
    }

    /**
     * Generates the part of the sql query specifying COUNT sql function
     *
     * @return string
     */
    private function getCountField(){
        $returnValue = '';

        foreach ($this->meta->fields as $metaField){
            $returnValue .= 'COUNT(' . $metaField->name . ') as Count ';
            break;
        }

        return($returnValue);
    }

    /**
     * Generates the part of the sql query specifying the fields to be returned
     *
     * @return string
     */
    private function getSelectedFields(){
        $returnValue = '*';
        if (isset($this->selectedFields) && sizeof($this->selectedFields) > 0){
            $returnValue = '';
            foreach ($this->selectedFields as $metaField){
                if ($metaField['sql'] && $metaField['sql'] != ''){
                    $returnValue .= $metaField['sql'] . '(' . $metaField['field']->name . ') as ' . $metaField['field']->name . ', ';
                } else {
                    $returnValue .= $metaField['field']->name . ', ';
                }
            }
            $returnValue = substr($returnValue, 0, strlen($returnValue) - 2);
        }
        return($returnValue);
    }

    /**
     * Generates the part of the sql query specifying the virtual fields. A virtual field is a field that is generated
     * by a query, but that is not a real field in the table
     *
     * @return string
     */
    private function getDynamicFields(){
        $returnValue = '';

        if (isset($this->dynamicFields) && sizeof($this->dynamicFields) > 0){
            foreach($this->dynamicFields as $fieldName=>$field){
                $returnValue .= ','.$field.' AS '.$fieldName;
            }
        }

        return($returnValue);
    }

    /**
     * Generates the part of the sql query specifying the HAVING clause
     *
     * @return string
     */
    private function getHavingClause(){
        $returnValue = '';
        if (isset($this->dynamicDiscriminant) && sizeof($this->dynamicDiscriminant) > 0){
            $returnValue .= ' HAVING ';
            $returnValue .= $this->getWhereAndHavingClause();
        }

        return($returnValue);
    }

    /**
     * Generates the part of the sql query specifying the WHERE clause
     *
     * @return string
     */
    private function getWhereClause(){
        $returnValue = '';

        if (isset($this->discriminants) && sizeof($this->discriminants) > 0){
            $returnValue .= ' WHERE ';
            $returnValue .= $this->getWhereAndHavingClause();
        }

        return($returnValue);
    }

    /**
     * Generates the part of the sql query specifying the ORDER BY clause
     *
     * @return string
     */
    private function getOrderClause(){
        $returnValue = '';

        if (isset($this->ordering) && sizeof($this->ordering) > 0){
            $returnValue = ' ORDER BY ';
            foreach ($this->ordering as $order){
                $field = $order[0];

                $returnValue .= $field->name;
                if ($order[1]){
                    $returnValue .= ' DESC';
                }
                $returnValue .= ', ';
            }
            $returnValue = substr($returnValue, 0, strlen($returnValue)-2);
        }

        return($returnValue);
    }

    /**
     * Generates the part of the sql query specifying the GROUP BY clause
     *
     * @return string
     */
    public function getGroupingClause(){
        $returnValue = '';

        if (isset($this->groupingFields) && sizeof($this->groupingFields) > 0){
            $returnValue = ' GROUP BY ';
            foreach ($this->groupingFields as $field){
                $returnValue .= $field->name . ', ';
            }
            $returnValue = substr($returnValue, 0, strlen($returnValue)-2);
        }

        return($returnValue);
    }

    /**
     * Returns the list of fields defining the record-identifying group during a UPDATE sql statement
     *
     * @return string
     */
    private function getKeyUpdateFields(){
        $returnValue = '';

        foreach($this->keyFields as $discriminant){
            $returnValue .= $discriminant->metaField->name . $discriminant->clause . '? AND ';
        }

        $returnValue = substr($returnValue, 0, strlen($returnValue)-5);

        return($returnValue);
    }

    /**
     * Returns the normal fields used during an UPDATE sql statement
     *
     * @return string
     */
    private function getNormalUpdateFields(){
        $returnValue = '';

        foreach($this->normalFields as $discriminant){
            $returnValue .= $discriminant->metaField->name . $discriminant->clause . '?, ';
        }

        $returnValue = substr($returnValue, 0, strlen($returnValue)-2);

        return($returnValue);
    }

    /**
     * Returns the fields used during an INSERT ssql statement
     *
     * @param string $fieldParams
     * @return string
     */
    private function getInsertFields(&$fieldParams) {
        $returnValue = '';

        foreach ($this->keyFields as $discriminant) {
            if (!$discriminant->metaField->isAutoNumbering) {
                $returnValue .= $discriminant->metaField->name . ', ';
                $fieldParams .= '?, ';
            }
        }

        foreach ($this->normalFields as $discriminant) {
            $returnValue .= $discriminant->metaField->name . ', ';
            $fieldParams .= '?, ';
        }

        if (strlen($returnValue) > 2) {
            $returnValue = substr($returnValue, 0, strlen($returnValue) - 2);
            $fieldParams = substr($fieldParams, 0, strlen($fieldParams) - 2);
        }

        return ($returnValue);
    }

    /**
     * Returns the type of field to be passed as type of parameters to mySql for the sql query preparation
     *
     * @param metaField $field
     * @return string
     */
    private function getDiscriminantTypeCast(metaField $field){
        switch ($field->type){
            case 'int':
            case 'tinyint':
                $returnValue = 'i';
                break;
            case 'decimal':
            case 'double':
            case 'float':
                $returnValue = 'd';
                break;
            case 'text':
            case 'varchar':
                $returnValue = 's';
                break;
            default:
                $returnValue = 'b';
                break;
        }

        return($returnValue);
    }

    /**
     * Returns the WHERE and HAVING common part of the clause
     *
     * @return string
     */
    private function getWhereAndHavingClause(){
        /**
         * @var discriminant $discriminant
         */
        $returnValue = '';

        for ($discriminantKey=0; $discriminantKey<sizeof($this->discriminants); $discriminantKey++){
            $discriminant = $this->discriminants[$discriminantKey];
            if (substr($discriminant->separator, 0, 1) == '('){
                $returnValue .= $discriminant->separator;
            }
            $returnValue .= $discriminant->metaField->name;

            if ($discriminant->clause == '=' && !isset($discriminant->value)){
                $returnValue .= ' IS NULL';
            } else {
                if (($discriminant->clause == '!=' || $discriminant->clause == '<>') && !isset($discriminant->value)){
                    $returnValue .= ' IS NOT NULL';
                } else {
                    if ($discriminant->clause == ' *LIKE '){
                        $returnValue .= ' LIKE ';
                    } else if ($discriminant->clause == ' LIKE* '){
                        $returnValue .= ' LIKE ';
                    } else {
                        $returnValue .= $discriminant->clause;
                    }
                }
            }

            if ($discriminant->clause == ' LIKE '){
                $returnValue .= "'%" . $discriminant->value . "%'";
                unset($this->discriminants[$discriminantKey]);
            } else if ($discriminant->clause == ' *LIKE '){
                $returnValue .= "'%" . $discriminant->value . "'";
                unset($this->discriminants[$discriminantKey]);
            } else if ($discriminant->clause == ' LIKE* '){
                $returnValue .= "'" . $discriminant->value . "%'";
                unset($this->discriminants[$discriminantKey]);
            } else if ($discriminant->clause == ' IN '){
                $returnValue .= '(' . $discriminant->value . ')';
                unset($this->discriminants[$discriminantKey]);
            } else if ($discriminant->clause == ' NOT IN '){
                $returnValue .= '(' . $discriminant->value . ')';
                unset($this->discriminants[$discriminantKey]);
            } else {
                if (isset($discriminant->value)){
                    if (is_object($discriminant->value) && $discriminant->value instanceof metaField){
                        $returnValue .= $discriminant->value->name;
                    } else {
                        $returnValue .= '?';
                    }
                }
            }

            if (substr($discriminant->separator, 0, 1) == ')'){
                $returnValue .= $discriminant->separator;
            }
            $returnValue .= $discriminant->connector;
        }
        if (substr($returnValue, strlen($returnValue) - 5) == ' AND '){
            $returnValue = substr($returnValue, 0, strlen($returnValue) - 5);
        } else if (substr($returnValue, strlen($returnValue) - 4) == ' OR '){
            $returnValue = substr($returnValue, 0, strlen($returnValue) - 4);
        }

        return($returnValue);
    }
}
?>