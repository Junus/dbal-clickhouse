<?php

declare(strict_types=1);

/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license inflormation, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse;

use ClickHouseDB\Client;
use Doctrine\DBAL\Driver\Result;
use Doctrine\DBAL\Driver\Statement;
use Doctrine\DBAL\ParameterType;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use function array_key_exists;
use function array_keys;
use function array_map;
use function array_replace;
use function array_walk;
use function current;
use function explode;
use function implode;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function preg_replace;
use function stripos;
use function trim;

/**
 * ClickHouse Statement
 */
class ClickHouseStatement implements \IteratorAggregate, Statement
{
    /** @var Client */
    protected $smi2CHClient;

    /** @var string */
    protected $sql;

    /** @var AbstractPlatform */
    protected $platform;

    /** @var mixed[] */
    protected $rows = [];

    /**
     * Query parameters for prepared statement (key => value)
     * @var mixed[]
     */
    protected $values = [];

    /**
     * Query parameters' types for prepared statement (key => value)
     * @var mixed[]
     */
    protected $types = [];

    /** @var \ArrayIterator|null */
    protected $iterator;

    public function __construct(Client $client, string $sql, AbstractPlatform $platform)
    {
        $this->smi2CHClient = $client;
        $this->sql          = $sql;
        $this->platform     = $platform;
    }

    /**
     * {@inheritDoc}
     */
    public function getIterator() : \ArrayIterator
    {
        if (! $this->iterator) {
            $this->iterator = new \ArrayIterator($this->rows);
        }

        return $this->iterator;
    }

    /**
     * {@inheritDoc}
     */
    public function bindValue($param, $value, $type = null)
    {
        $this->values[$param] = $value;
        $this->types[$param]  = $type;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function bindParam($param, &$variable, $type = null, $length = null)
    {
        $this->values[$param] = &$variable;
        $this->types[$param]  = $type;
        return true;
    }

    /**
     * {@inheritDoc}
     */
    public function execute($params = null) : Result
    {
        if (is_array($params)) {
            $this->values = array_replace($this->values, $params);//TODO array keys must be all strings or all integers?
        }

        $sql = $this->sql;

        $numericKeys = [];
        $wordKeys = [];

        foreach (array_keys($this->values) as $key) {
            if (is_int($key)) {
                $numericKeys[] = $key;
            } else {
                $wordKeys[] = $key;
            }
        }

        $wordKeyPatterns = [];
        if (count($wordKeys)) {
            $wordKeyPatterns = array_map(function (string $key) : string {
                return ':' . preg_quote($key, '/');
            }, $wordKeys);
        }

        $keyPattern = implode('|', array_merge(['\?'], $wordKeyPatterns));

        $keyIndex = 0;
        $sql = preg_replace_callback(
            '/(' . $keyPattern . ')/i',
            function (array $matches) use ($numericKeys, &$keyIndex) : string {
                $key = $matches[0];
                if ($key === '?') {
                    if (!array_key_exists($keyIndex, $numericKeys)) {
                        return '?'; // maybe ternary operator, clickhouse supports it
                    }
                    $key = $numericKeys[$keyIndex];
                    $keyIndex++;
                } else {
                    $key = ltrim($key, ':');
                }

                return $this->getTypedParam($key);
            },
            $sql
        );

        $this->processViaSMI2($sql);

        return new ClickHouseResult($this);
    }

    /**
     * Specific SMI2 ClickHouse lib statement execution
     * If you want to use any other lib for working with CH -- just update this method
     *
     */
    protected function processViaSMI2(string $sql) : void
    {
        $sql = trim($sql);

        $this->rows =
            stripos($sql, 'select') === 0 ||
            stripos($sql, 'show') === 0 ||
            stripos($sql, 'describe') === 0 ?
                $this->smi2CHClient->select($sql)->rows() :
                $this->smi2CHClient->write($sql)->rows();
    }

    /**
     * @param string|int $key
     * @throws ClickHouseException
     */
    protected function getTypedParam($key) : string
    {
        if ($this->values[$key] === null) {
            return 'NULL';
        }

        $type = $this->types[$key] ?? null;

        // if param type was not setted - trying to get db-type by php-var-type
        if ($type === null) {
            if (is_bool($this->values[$key])) {
                $type = ParameterType::BOOLEAN;
            } elseif (is_int($this->values[$key]) || is_float($this->values[$key])) {
                $type = ParameterType::INTEGER;
            } elseif (is_array($this->values[$key])) {
                /*
                 * ClickHouse Arrays
                 */
                $values = $this->values[$key];
                if (is_int(current($values)) || is_float(current($values))) {
                    array_map(
                        function ($value) : void {
                            if (! is_int($value) && ! is_float($value)) {
                                throw new ClickHouseException(
                                    'Array values must all be int/float or string, mixes not allowed'
                                );
                            }
                        },
                        $values
                    );
                } else {
                    $values = array_map(function ($value) {
                        return $value === null ? 'NULL' : $this->platform->quoteStringLiteral($value);
                    }, $values);
                }

                return '[' . implode(', ', $values) . ']';
            }
        }

        if ($type === ParameterType::INTEGER) {
            return (string) $this->values[$key];
        }

        if ($type === ParameterType::BOOLEAN) {
            return (string) (int) (bool) $this->values[$key];
        }

        return $this->platform->quoteStringLiteral((string) $this->values[$key]);
    }

    public function freeResult()
    {
        $this->rows = [];
        $this->values = [];
        $this->types = [];
        $this->iterator = null;
    }
}