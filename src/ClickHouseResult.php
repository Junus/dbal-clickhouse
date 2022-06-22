<?php

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\Driver\FetchUtils;
use Doctrine\DBAL\Driver\Result;

class ClickHouseResult implements Result
{
    /**
     * @var ClickHouseStatement
     */
    private $statement;

    public function __construct(ClickHouseStatement $statement)
    {
        $this->statement = $statement;
    }

    /**
     * @inheritDoc
     */
    public function fetchNumeric()
    {
        $data = $this->statement->getIterator()->current();
        if (is_null($data)) {
            return false;
        }
        $this->statement->getIterator()->next();
        return array_values($data);
    }

    /**
     * @inheritDoc
     */
    public function fetchAssociative()
    {
        $data = $this->statement->getIterator()->current();
        if (is_null($data)) {
            return false;
        }
        $this->statement->getIterator()->next();
        return $data;
    }

    /**
     * @inheritDoc
     */
    public function fetchOne()
    {
        return FetchUtils::fetchOne($this);
    }

    /**
     * @inheritDoc
     */
    public function fetchAllNumeric(): array
    {
        return FetchUtils::fetchAllNumeric($this);
    }

    /**
     * @inheritDoc
     */
    public function fetchAllAssociative(): array
    {
        return FetchUtils::fetchAllAssociative($this);
    }

    /**
     * @inheritDoc
     */
    public function fetchFirstColumn(): array
    {
        return FetchUtils::fetchFirstColumn($this);
    }

    /**
     * @inheritDoc
     */
    public function rowCount(): int
    {
        return 1;
    }

    /**
     * @inheritDoc
     */
    public function columnCount(): int
    {
        return $this->statement->getIterator()->count();
    }

    /**
     * @inheritDoc
     */
    public function free(): void
    {
        $this->statement->freeResult();
    }
}