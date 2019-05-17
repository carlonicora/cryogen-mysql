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

use carlonicora\cryogen\SQL\sqlQueryEngine;

/**
 * The mySqlQueryEngine specialises the queryEngine to prepare the SQL queries specifically for MySql
 */
class mysqlQueryEngine extends sqlQueryEngine{
    /**
     * Returns the type of field to be passed as type of parameters to mySql for the sql query preparation
     *
     * @param mixed $field
     * @return string
     */
    protected function getDiscriminantTypeCast($field=null){
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
            case 'char':
            case 'timestamp':
                $returnValue = 's';
                break;
            default:
                $returnValue = 'b';
                break;
        }

        return($returnValue);
    }
}