<?php

namespace Laminas\Db\Adapter\Driver\Pdo;

use Laminas\Db\Adapter\Driver\StatementInterface;
use Laminas\Db\Adapter\Exception;
use Laminas\Db\Adapter\ParameterContainer;
use Laminas\Db\Adapter\Profiler;
use PDOException;
use PDOStatement;

use function implode;
use function is_array;
use function is_bool;
use function is_int;

class Statement implements StatementInterface, Profiler\ProfilerAwareInterface
{
    /** @var \PDO */
    protected $pdo;

    /** @var Profiler\ProfilerInterface */
    protected $profiler;

    /** @var Pdo */
    protected $driver;

    /** @var string */
    protected $sql = '';

    /** @var bool */
    protected $isQuery;

    /** @var ParameterContainer */
    protected $parameterContainer;

    /** @var bool */
    protected $parametersBound = false;

    /** @var PDOStatement */
    protected $resource;

    /** @var bool */
    protected $isPrepared = false;
    
    // ALAN
    // @var array
    protected $colLengths = [];
    
    // ALAN
    // @var array
    protected $colTypes = [];
    
    // DAVE
    // @var string
    protected $sprocName = [];
    
    
    /**
     * Set driver
     *
     * @return $this Provides a fluent interface
     */
    public function setDriver(Pdo $driver)
    {
        $this->driver = $driver;
        return $this;
    }

    /**
     * @return $this Provides a fluent interface
     */
    public function setProfiler(Profiler\ProfilerInterface $profiler)
    {
        $this->profiler = $profiler;
        return $this;
    }

    /**
     * @return null|Profiler\ProfilerInterface
     */
    public function getProfiler()
    {
        return $this->profiler;
    }

    /**
     * Initialize
     *
     * @return $this Provides a fluent interface
     */
    public function initialize(\PDO $connectionResource)
    {
        $this->pdo = $connectionResource;
        return $this;
    }

    /**
     * Set resource
     *
     * @return $this Provides a fluent interface
     */
    public function setResource(PDOStatement $pdoStatement)
    {
        $this->resource = $pdoStatement;
        return $this;
    }

    /**
     * Get resource
     *
     * @return mixed
     */
    public function getResource()
    {
        return $this->resource;
    }

    /**
     * Set sql
     *
     * @param string $sql
     * @return $this Provides a fluent interface
     */
    public function setSql($sql)
    {
        $this->sql = $sql;
        return $this;
    }

    /**
     * Get sql
     *
     * @return string
     */
    public function getSql()
    {
        return $this->sql;
    }

    /**
     * @return $this Provides a fluent interface
     */
    public function setParameterContainer(ParameterContainer $parameterContainer)
    {
        $this->parameterContainer = $parameterContainer;
        return $this;
    }

    /**
     * @return ParameterContainer
     */
    public function getParameterContainer()
    {
        return $this->parameterContainer;
    }

    /**
     * @param string $sql
     * @throws Exception\RuntimeException
     */
    public function prepare($sql = null)
    {
        if ($this->isPrepared) {
            throw new Exception\RuntimeException('This statement has been prepared already');
        }

        if ($sql === null) {
            $sql = $this->sql;
        }
        // begin AS
        // get params
        $isSproc = preg_match('\'CALL (.*)[\\(]\'', $sql, $matches);
        if ($isSproc) {
            $sprocName = strtoupper($matches[1]);
            $library = '';
            if(strpos(strtolower($_ENV['application_environment']), 'test') !== false || 
               strpos(strtolower($_ENV['application_environment']), 'development') !== false){
                $library = $_ENV['library_development'];
            } else if(strpos(strtolower($_ENV['application_environment']), 'prod') !== false){
                $library = $_ENV['library_production'];
            }
            $colSql = "select ORDINAL_POSITION, CHARACTER_MAXIMUM_LENGTH, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE "
                    . "from sysparms where SPECIFIC_NAME = '$sprocName' and SPECIFIC_SCHEMA = '$library'";
            $colStmt = $this->pdo->prepare($colSql);
            $colResult = $colStmt->execute();
            $rows = $colStmt->fetchAll();
            $this->colLengths = [];
            $this->colTypes = [];
            $this->sprocName = $sprocName;
            $numeric_datatype_array = array('SMALLINT','BIGINT','DOUBLE PRECISION','DECIMAL','REAL','NUMERIC','INTEGER');

            foreach ($rows as $row) {
                $position = $row['ORDINAL_POSITION'];
                $exportRow = var_export($row,true);

                if(in_array($row['DATA_TYPE'],$numeric_datatype_array)) {

                    $this->colLengths[$position] = $row['NUMERIC_PRECISION'];
                    if(!is_null($row['NUMERIC_SCALE']) && $row['NUMERIC_SCALE'] > 0) {
                        $this->colLengths[$position]++;
                    }
                } else {
                    $length = $row['CHARACTER_MAXIMUM_LENGTH'];
                    $this->colLengths[$position] = $length;
                }
                $type = $row['DATA_TYPE'];
                $this->colTypes[$position] = $type;
            }
        }
        // end AS
        
        $this->resource = $this->pdo->prepare($sql);

        if ($this->resource === false) {
            $error = $this->pdo->errorInfo();
            throw new Exception\RuntimeException($error[2]);
        }

        $this->isPrepared = true;
    }

    /**
     * @return bool
     */
    public function isPrepared()
    {
        return $this->isPrepared;
    }

    /**
     * @param null|array|ParameterContainer $parameters
     * @throws Exception\InvalidQueryException
     * @return Result
     */
    public function execute($parameters = null, $directions = null)
    {
        
        
        // which constants indicate output.
        $outputConstantValues = array(DB2_PARAM_OUT, DB2_PARAM_INOUT);
        
        if (! $this->isPrepared) {
            $this->prepare();
        }

        /** START Standard ParameterContainer Merging Block */
        if (! $this->parameterContainer instanceof ParameterContainer) {
            if ($parameters instanceof ParameterContainer) {
                $this->parameterContainer = $parameters;
                $parameters               = null;
            } else {
                $this->parameterContainer = new ParameterContainer();
            }
        }

        if (is_array($parameters)) {
            $this->parameterContainer->setFromArray($parameters);
        }

        if ($this->parameterContainer->count() > 0) {
            if (!$this->parametersBound) {
                $parameters = $this->parameterContainer->getNamedArray();
                $directionsValues = array_values($directions);
                $idx=0;
                foreach ($parameters as $name => &$value) {
                    if (is_bool($value)) {
                        $type = \PDO::PARAM_BOOL;
                    } elseif (is_int($value)) {
                        $type = \PDO::PARAM_INT;
                    } else {
                        $type = \PDO::PARAM_STR;
                    }
                    if ($this->parameterContainer->offsetHasErrata($name)) {
                        switch ($this->parameterContainer->offsetGetErrata($name)) {
                            case ParameterContainer::TYPE_INTEGER:
                                $type = \PDO::PARAM_INT;
                                break;
                            case ParameterContainer::TYPE_NULL:
                                $type = \PDO::PARAM_NULL;
                                break;
                            case ParameterContainer::TYPE_LOB:
                                $type = \PDO::PARAM_LOB;
                                break;
                        }
                    }
                    
                    $paramPosition = $idx+1; // 1-based index. AS made this
                    //If a directions array was passed in then bind output parameters
                    if ($directions){
                        $maxLength = $this->colLengths[$paramPosition] + 1; // for ODBC
                        $colType = $this->colTypes[$paramPosition]; // for ODBC
                        if(in_array($directionsValues[$idx], $outputConstantValues)){ // compat with ibm_db2 caller even if constant not defined.
                            if ($colType == 'CHARACTER' or $colType == 'CHARACTER VARYING') {
                                $this->resource->bindParam($paramPosition, $value, $type|\PDO::PARAM_INPUT_OUTPUT, $maxLength);
                            } else if ($this->sprocName == $_ENV['stored_proc_name_for_out_param_check1'] && $paramPosition==5 ) {
                                $this->resource->bindParam($paramPosition, $value, $type|\PDO::PARAM_INPUT_OUTPUT, 9);
                            } else if ($this->sprocName == $_ENV['stored_proc_name_for_out_param_check2'] && $paramPosition==4 ) {
                                $this->resource->bindParam($paramPosition, $value, $type|\PDO::PARAM_INPUT_OUTPUT, 9);
                            } else {
                                $this->resource->bindParam($paramPosition, $value, $type|\PDO::PARAM_INPUT_OUTPUT);
                            }
                            
                        }
                        //It's an input parameter
                        else {
                            
                            $this->resource->bindParam($paramPosition, $value, $type);
                            
                        }
                    }
                    else{
                        $this->resource->bindParam($paramPosition, $value, $type);
                    }
                    $idx++;
                } //(foreach)
            }
        }
        /** END Standard ParameterContainer Merging Block */
        
        if ($this->profiler) {
            $this->profiler->profilerStart($this);
        }
        
        try {
            $this->resource->execute();
        } catch (\PDOException $e) {
            if ($this->profiler) {
                $this->profiler->profilerFinish();
            }
            throw new Exception\InvalidQueryException(
                'Statement could not be executed (' . implode(' - ', $this->resource->errorInfo()) . ')',
                null,
                $e
                );
        }
        
        if (is_array($parameters) || is_object($parameters))
        {
            foreach ($parameters as $name => &$value) {
                $this->parameterContainer->offsetSet ( $name, $value);
            }
        }
        
        if ($this->profiler) {
            $this->profiler->profilerFinish();
        }
        
        $result = $this->driver->createResult($this->resource, $this);
        
        return $result;
    }

    /**
     * Perform a deep clone
     *
     * @return void
     */
    public function __clone()
    {
        $this->isPrepared      = false;
        $this->parametersBound = false;
        $this->resource        = null;
        if ($this->parameterContainer) {
            $this->parameterContainer = clone $this->parameterContainer;
        }
    }
}
