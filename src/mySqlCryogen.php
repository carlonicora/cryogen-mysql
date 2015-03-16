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

/**
 * Main class for the MySql plugin for cryogen
 *
 * @package CarloNicora\cryogen\mySqlCryogen
 */
class mySqlCryogen extends cryogen{
    /**
     *
     * @param array $connectionString
     */
    public function __construct($connectionString){
        $returnValue = false;

        $this->connectionController = new mySqlConnectionController();

        if ($this->connectionController->initialize($connectionString)){
            $this->dataController = new mySqlDataController($this->connectionController, $this);
            $this->structureController = new mySqlStructureController($this->connectionController, $this);
            $returnValue = true;
        }

        return($returnValue);
    }

    public function generateQueryEngine($meta = NULL, $entity = NULL, $valueOfKeyField = NULL){
        $returnValue = NULL;

        $this->clearLastLogs();

        $returnValue = new mySqlQueryEngine($meta, $entity, $valueOfKeyField);

        return($returnValue);
    }

    public function __destruct(){
        if (isset($this->connectionController) && $this->connectionController->isConnected()){
            $this->connectionController->disconnect();
        }
    }
}
?>