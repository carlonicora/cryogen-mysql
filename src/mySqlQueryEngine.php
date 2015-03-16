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

use CarloNicora\cryogen\queryEngine;
use CarloNicora\cryogen\metaField;

class mySqlQueryEngine extends queryEngine{

    public function generateReadStatement(){
        return("SELECT " . $this->getSelectedFields() . $this->getDynamicFields() . " FROM " . $this->meta->name . $this->getWhereClause() . $this->getHavingClause() . $this->getGroupingClause() . $this->getOrderClause() . $this->getLimitClause() . ";");
    }

    public function generateReadCountStatement(){
        return("SELECT " . $this->getCountField() . " FROM " . $this->meta->name . $this->getWhereClause() . ";");
    }

    private function getLimitClause(){
        $returnValue = "";

        if ($this->limitStart || $this->limitLength){
            $returnValue = " LIMIT " . $this->limitStart . "," . $this->limitLength;
        }

        return($returnValue);
    }

    private function getCountField(){
        $returnValue = "";

        foreach ($this->meta->fields as $metaField){
            $returnValue .= "COUNT(" . $metaField->name . ") as Count ";
            break;
        }

        return($returnValue);
    }

    private function getSelectedFields(){
        $returnValue = "*";
        if (isset($this->selectedFields) && sizeof($this->selectedFields) > 0){
            $returnValue = "";
            foreach ($this->selectedFields as $metaField){
                if ($metaField['sql'] && $metaField['sql'] != ""){
                    $returnValue .= $metaField['sql'] . "(" . $metaField['field']->name . ") as " . $metaField['field']->name . ", ";
                } else {
                    $returnValue .= $metaField['field']->name . ", ";
                }
            }
            $returnValue = substr($returnValue, 0, strlen($returnValue) - 2);
        }
        return($returnValue);
    }

    private function getDynamicFields(){
        $returnValue = '';

        if (isset($this->dynamicFields) && sizeof($this->dynamicFields) > 0){
            foreach($this->dynamicFields as $fieldName=>$field){
                $returnValue .= ','.$field.' AS '.$fieldName;
            }
        }

        return($returnValue);
    }

    private function getHavingClause(){
        $returnValue = '';
        if (isset($this->dynamicDiscriminant) && sizeof($this->dynamicDiscriminant) > 0){
            $returnValue .= " HAVING ";
            $actualElement = 0;
            foreach ($this->dynamicDiscriminant as $discriminant){
                if (substr($discriminant->separator, 0, 1) == "("){
                    $returnValue .= $discriminant->separator;
                }
                $returnValue .= $discriminant->fieldName;
                if ($discriminant->clause == "=" && !isset($discriminant->value)){
                    $returnValue .= " IS NULL";
                } else {
                    if (($discriminant->clause == "!=" || $discriminant->clause == "<>") && !isset($discriminant->value)){
                        $returnValue .= " IS NOT NULL";
                    } else {
                        if ($discriminant->clause == " *LIKE "){
                            $returnValue .= " LIKE ";
                        } else if ($discriminant->clause == " LIKE* "){
                            $returnValue .= " LIKE ";
                        } else {
                            $returnValue .= $discriminant->clause;
                        }
                    }
                }

                if ($discriminant->clause == " LIKE "){
                    $returnValue .= "'%" . $discriminant->value . "%'";
                    //$returnValue .= "*?*";
                    array_splice($this->discriminants, $actualElement, 1);
                } else if ($discriminant->clause == " *LIKE "){
                    $returnValue .= "'%" . $discriminant->value . "'";
                    //$returnValue .= "*?*";
                    array_splice($this->discriminants, $actualElement, 1);
                } else if ($discriminant->clause == " LIKE* "){
                    $returnValue .= "'" . $discriminant->value . "%'";
                    //$returnValue .= "*?*";
                    array_splice($this->discriminants, $actualElement, 1);
                } else if ($discriminant->clause == " IN "){
                    $returnValue .= "(" . $discriminant->value . ")";
                    array_splice($this->discriminants, $actualElement, 1);
                } else if ($discriminant->clause == " NOT IN "){
                    $returnValue .= "(" . $discriminant->value . ")";
                    array_splice($this->discriminants, $actualElement, 1);
                } else {
                    if (isset($discriminant->value)){
                        if (is_object($discriminant->value) && $discriminant->value instanceof metaField){
                            $returnValue .= $discriminant->value->name;
                        } else {
                            $returnValue .= "?";
                        }
                    }
                    $actualElement++;
                }
                if (substr($discriminant->separator, 0, 1) == ")"){
                    $returnValue .= $discriminant->separator;
                }
                $returnValue .= $discriminant->connector;
            }
            if (substr($returnValue, strlen($returnValue) - 5) == " AND "){
                $returnValue = substr($returnValue, 0, strlen($returnValue) - 5);
            } else if (substr($returnValue, strlen($returnValue) - 4) == " OR "){
                $returnValue = substr($returnValue, 0, strlen($returnValue) - 4);
            }
        }

        return($returnValue);
    }

    private function getWhereClause(){
        $returnValue = "";

        if (isset($this->discriminants) && sizeof($this->discriminants) > 0){
            $returnValue .= " WHERE ";
            $actualElement = 0;
            foreach ($this->discriminants as $discriminant){
                if (substr($discriminant->separator, 0, 1) == "("){
                    $returnValue .= $discriminant->separator;
                }
                $returnValue .= $discriminant->metaField->name;
                if ($discriminant->clause == "=" && !isset($discriminant->value)){
                    $returnValue .= " IS NULL";
                } else {
                    if (($discriminant->clause == "!=" || $discriminant->clause == "<>") && !isset($discriminant->value)){
                        $returnValue .= " IS NOT NULL";
                    } else {
                        if ($discriminant->clause == " *LIKE "){
                            $returnValue .= " LIKE ";
                        } else if ($discriminant->clause == " LIKE* "){
                            $returnValue .= " LIKE ";
                        } else {
                            $returnValue .= $discriminant->clause;
                        }
                    }
                }

                if ($discriminant->clause == " LIKE "){
                    $returnValue .= "'%" . $discriminant->value . "%'";
                    //$returnValue .= "*?*";
                    array_splice($this->discriminants, $actualElement, 1);
                } else if ($discriminant->clause == " *LIKE "){
                    $returnValue .= "'%" . $discriminant->value . "'";
                    //$returnValue .= "*?*";
                    array_splice($this->discriminants, $actualElement, 1);
                } else if ($discriminant->clause == " LIKE* "){
                    $returnValue .= "'" . $discriminant->value . "%'";
                    //$returnValue .= "*?*";
                    array_splice($this->discriminants, $actualElement, 1);
                } else if ($discriminant->clause == " IN "){
                    $returnValue .= "(" . $discriminant->value . ")";
                    array_splice($this->discriminants, $actualElement, 1);
                } else if ($discriminant->clause == " NOT IN "){
                    $returnValue .= "(" . $discriminant->value . ")";
                    array_splice($this->discriminants, $actualElement, 1);
                } else {
                    if (isset($discriminant->value)){
                        $returnValue .= "?";
                    }
                    $actualElement++;
                }
                if (substr($discriminant->separator, 0, 1) == ")"){
                    $returnValue .= $discriminant->separator;
                }
                $returnValue .= $discriminant->connector;
            }
            if (substr($returnValue, strlen($returnValue) - 5) == " AND "){
                $returnValue = substr($returnValue, 0, strlen($returnValue) - 5);
            } else if (substr($returnValue, strlen($returnValue) - 4) == " OR "){
                $returnValue = substr($returnValue, 0, strlen($returnValue) - 4);
            }
        }

        return($returnValue);
    }

    private function getOrderClause(){
        $returnValue = "";

        if (isset($this->ordering)){
            $returnValue = " ORDER BY ";
            foreach ($this->ordering as $order){
                $field = $order[0];

                $returnValue .= $field->name;
                if ($order[1]){
                    $returnValue .= " DESC";
                }
                $returnValue .= ", ";
            }
            $returnValue = substr($returnValue, 0, strlen($returnValue)-2);
        }

        return($returnValue);
    }

    public function getGroupingClause(){
        $returnValue = "";

        if ($this->groupingFields && sizeof($this->groupingFields) > 0){
            $returnValue = " GROUP BY ";
            foreach ($this->groupingFields as $field){
                $returnValue .= $field->name . ", ";
            }
            $returnValue = substr($returnValue, 0, strlen($returnValue)-2);
        }

        return($returnValue);
    }

    public function generateReadParameters(){
        $returnValue = [];

        if (isset($this->discriminants) && sizeof($this->discriminants) > 0){
            $returnValue[0] = '';
            $returnValue[1] = [];
            foreach ($this->discriminants as $discriminant){
                if (isset($discriminant->value)){
                    switch ($discriminant->metaField->type){
                        case "int":
                        case "tinyint":
                            $returnValue[0] .= 'i';
                            break;
                        case "decimal":
                        case "double":
                        case "float":
                            $returnValue[0] .= 'd';
                            break;
                        case "text":
                        case "varchar":
                            $returnValue[0] .= 's';
                            break;
                        default:
                            $returnValue[0] .= 'b';
                            break;
                    }
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
                        switch ($discriminant->type) {
                            case "int":
                            case "tinyint":
                                $returnValue[0] .= 'i';
                                break;
                            case "decimal":
                            case "double":
                            case "float":
                                $returnValue[0] .= 'd';
                                break;
                            case "text":
                            case "varchar":
                                $returnValue[0] .= 's';
                                break;
                            default:
                                $returnValue[0] .= 'b';
                                break;
                        }
                        $returnValue[1][] = $discriminant->value;
                    }
                }
            }
        }

        return($returnValue);
    }

    public function generateUpdateStatement(){
        $returnValue = "UPDATE " . $this->meta->name . " SET " . $this->getNormalUpdateFields() . " WHERE " . $this->getKeyUpdateFields() . ";";

        return($returnValue);
    }

    public function generateUpdateParameters(){
        $returnValue = [];

        $returnValue[0] = '';

        if (isset($this->normalFields) && sizeof($this->normalFields) > 0){
            if (!isset($returnValue[1])){
                $returnValue[1] = [];
            }
            foreach ($this->normalFields as $discriminant){
                switch ($discriminant->metaField->type){
                    case "int":
                    case "tinyint":
                        $returnValue[0] .= 'i';
                        break;
                    case "decimal":
                    case "double":
                    case "float":
                        $returnValue[0] .= 'd';
                        break;
                    case "varchar":
                    case "text":
                        $returnValue[0] .= 's';
                        break;
                    default:
                        $returnValue[0] .= 'b';
                        break;
                }
                $returnValue[1][] = $discriminant->value;
            }
        }

        if (isset($this->keyFields) && sizeof($this->keyFields) > 0){
            if (!isset($returnValue[1])){
                $returnValue[1] = [];
            }
            foreach ($this->keyFields as $discriminant){
                switch ($discriminant->metaField->type){
                    case "int":
                    case "tinyint":
                        $returnValue[0] .= 'i';
                        break;
                    case "decimal":
                    case "double":
                    case "float":
                        $returnValue[0] .= 'd';
                        break;
                    case "text":
                    case "varchar":
                        $returnValue[0] .= 's';
                        break;
                    default:
                        $returnValue[0] .= 'b';
                        break;
                }
                $returnValue[1][] = $discriminant->value;
            }
        }

        return($returnValue);
    }

    private function getKeyUpdateFields(){
        $returnValue = "";

        foreach($this->keyFields as $discriminant){
            $returnValue .= $discriminant->metaField->name . $discriminant->clause . "? AND ";
        }

        $returnValue = substr($returnValue, 0, strlen($returnValue)-5);

        return($returnValue);
    }

    private function getNormalUpdateFields(){
        $returnValue = "";

        foreach($this->normalFields as $discriminant){
            $returnValue .= $discriminant->metaField->name . $discriminant->clause . "?, ";
        }

        $returnValue = substr($returnValue, 0, strlen($returnValue)-2);

        return($returnValue);
    }

    public function generateInsertStatement(){
        $fieldParams = "";
        $returnValue = "INSERT INTO " . $this->meta->name . " (" . $this->getInsertFields($fieldParams) . ") VALUES (" . $fieldParams . ");";

        return($returnValue);
    }

    public function generateInsertParameters(){
        $returnValue = [];

        $returnValue[0] = '';

        if (isset($this->keyFields) && sizeof($this->keyFields) > 0){
            if (!isset($returnValue[1])){
                $returnValue[1] = [];
            }
            foreach ($this->keyFields as $discriminant){
                if (!$discriminant->metaField->isAutoNumbering){
                    switch ($discriminant->metaField->type){
                        case "int":
                        case "tinyint":
                            $returnValue[0] .= 'i';
                            break;
                        case "decimal":
                        case "double":
                        case "float":
                            $returnValue[0] .= 'd';
                            break;
                        case "varchar":
                        case "text":
                            $returnValue[0] .= 's';
                            break;
                        default:
                            $returnValue[0] .= 'b';
                            break;
                    }
                    $returnValue[1][] = $discriminant->value;
                }
            }
        }

        if (isset($this->normalFields) && sizeof($this->normalFields) > 0){
            if (!isset($returnValue[1])){
                $returnValue[1] = [];
            }
            foreach ($this->normalFields as $discriminant){
                switch ($discriminant->metaField->type){
                    case "int":
                    case "tinyint":
                        $returnValue[0] .= 'i';
                        break;
                    case "decimal":
                    case "double":
                    case "float":
                        $returnValue[0] .= 'd';
                        break;
                    case "varchar":
                    case "text":
                        $returnValue[0] .= 's';
                        break;
                    default:
                        $returnValue[0] .= 'b';
                        break;
                }
                $returnValue[1][] = $discriminant->value;
            }
        }

        return($returnValue);
    }

    private function getInsertFields(&$fieldParams){
        $returnValue = "";

        foreach($this->keyFields as $discriminant){
            if (!$discriminant->metaField->isAutoNumbering){
                $returnValue .= $discriminant->metaField->name . ", ";
                $fieldParams .= "?, ";
            }
        }

        foreach($this->normalFields as $discriminant){
            $returnValue .= $discriminant->metaField->name . ", ";
            $fieldParams .= "?, ";
        }

        if (strlen($returnValue) > 2){
            $returnValue = substr($returnValue, 0, strlen($returnValue)-2);
            $fieldParams = substr($fieldParams, 0, strlen($fieldParams)-2);
        }

        return($returnValue);
    }

    public function generateDeleteStatement(){
        //$returnValue = "";

        $returnValue = "DELETE FROM " . $this->meta->name . " WHERE ";
        $sqlGenerated = FALSE;

        $lastDiscriminant = "";

        if (isset($this->keyFields) && sizeof($this->keyFields) > 0){
            foreach($this->keyFields as $discriminant){
                if (isset($discriminant->value)){
                    $returnValue .= $discriminant->metaField->name . $discriminant->clause . "?" . $discriminant->connector;
                    $lastDiscriminant = $discriminant->connector;
                    $sqlGenerated = TRUE;
                }
            }
        }

        if (!$sqlGenerated) {
            if (isset($this->discriminants) && sizeof($this->discriminants) > 0){
                foreach($this->discriminants as $discriminant){
                    if (isset($discriminant->value)){
                        $returnValue .= $discriminant->metaField->name . $discriminant->clause . "?" . $discriminant->connector;
                        $lastDiscriminant = $discriminant->connector;
                    }
                }
            } else {
                foreach ($this->normalFields as $discriminant){
                    if (isset($discriminant->value)){
                        $returnValue .= $discriminant->metaField->name . $discriminant->clause . "?" . $discriminant->connector;
                        $lastDiscriminant = $discriminant->connector;
                    }
                }
            }
        }

        if (!$lastDiscriminant){
            $lastDiscriminant = "";
        }
        $returnValue = substr($returnValue, 0, strlen($returnValue)-strlen($lastDiscriminant));

        if (substr($returnValue, -7) == " WHERE "){
            $returnValue = substr($returnValue, 0, -7);
        }

        return($returnValue);
    }

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
                switch ($discriminant->metaField->type){
                    case "int":
                    case "tinyint":
                        $returnValue[0] .= 'i';
                        break;
                    case "decimal":
                    case "double":
                    case "float":
                        $returnValue[0] .= 'd';
                        break;
                    case "varchar":
                        $returnValue[0] .= 's';
                        break;
                    default:
                        $returnValue[0] .= 'b';
                        break;
                }
                $returnValue[1][] = $discriminant->value;
            }
        }
        return($returnValue);
    }
}
?>