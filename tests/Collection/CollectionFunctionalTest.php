<?php

namespace MongoDB\Tests\Collection;

use MongoDB\Collection;
use MongoDB\Driver\BulkWrite;
use MongoDB\Driver\ReadConcern;
use MongoDB\Driver\ReadPreference;
use MongoDB\Driver\WriteConcern;

/**
 * Functional tests for the Collection class.
 */
class CollectionFunctionalTest extends FunctionalTestCase
{
    /**
     * @expectedException MongoDB\Exception\InvalidArgumentException
     * @dataProvider provideInvalidNamespaceValues
     */
    public function testConstructorNamespaceArgument($namespace)
    {
        // TODO: Move to unit test once ManagerInterface can be mocked (PHPC-378)
        new Collection($this->manager, $namespace);
    }

    public function provideInvalidNamespaceValues()
    {
        return [
            [null],
            [''],
            ['db_collection'],
            ['db'],
            ['.collection'],
        ];
    }

    /**
     * @expectedException MongoDB\Exception\InvalidArgumentTypeException
     * @dataProvider provideInvalidConstructorOptions
     */
    public function testConstructorOptionTypeChecks(array $options)
    {
        new Collection($this->manager, $this->getNamespace(), $options);
    }

    public function provideInvalidConstructorOptions()
    {
        $options = [];

        foreach ($this->getInvalidReadPreferenceValues() as $value) {
            $options[][] = ['readPreference' => $value];
        }

        foreach ($this->getInvalidWriteConcernValues() as $value) {
            $options[][] = ['writeConcern' => $value];
        }

        return $options;
    }

    public function testToString()
    {
        $this->assertEquals($this->getNamespace(), (string) $this->collection);
    }

    public function getGetCollectionName()
    {
        $this->assertEquals($this->getCollectionName(), $this->collection->getCollectionName());
    }

    public function getGetDatabaseName()
    {
        $this->assertEquals($this->getDatabaseName(), $this->collection->getDatabaseName());
    }

    public function testGetNamespace()
    {
        $this->assertEquals($this->getNamespace(), $this->collection->getNamespace());
    }

    public function testDrop()
    {
        $writeResult = $this->collection->insertOne(['x' => 1]);
        $this->assertEquals(1, $writeResult->getInsertedCount());

        $commandResult = $this->collection->drop();
        $this->assertCommandSucceeded($commandResult);
        $this->assertCollectionCount($this->getNamespace(), 0);
    }

    /**
     * @expectedException MongoDB\Exception\InvalidArgumentException
     * @todo Move this to a unit test once Manager can be mocked
     */
    public function testDropIndexShouldNotAllowWildcardCharacter()
    {
        $this->collection->dropIndex('*');
    }

    public function testFindOne()
    {
        $this->createFixtures(5);

        $filter = ['_id' => ['$lt' => 5]];
        $options = [
            'skip' => 1,
            'sort' => ['x' => -1],
        ];

        $expected = (object) ['_id' => 3, 'x' => 33];

        $this->assertEquals($expected, $this->collection->findOne($filter, $options));
    }

    public function testWithOptionsInheritsReadPreferenceAndWriteConcern()
    {
        $collectionOptions = [
            'readConcern' => new ReadConcern(ReadConcern::LOCAL),
            'readPreference' => new ReadPreference(ReadPreference::RP_SECONDARY_PREFERRED),
            'writeConcern' => new WriteConcern(WriteConcern::MAJORITY),
        ];

        $collection = new Collection($this->manager, $this->getNamespace(), $collectionOptions);
        $clone = $collection->withOptions();
        $debug = $clone->__debugInfo();

        $this->assertInstanceOf('MongoDB\Driver\ReadConcern', $debug['readConcern']);
        $this->assertSame(ReadConcern::LOCAL, $debug['readConcern']->getLevel());
        $this->assertInstanceOf('MongoDB\Driver\ReadPreference', $debug['readPreference']);
        $this->assertSame(ReadPreference::RP_SECONDARY_PREFERRED, $debug['readPreference']->getMode());
        $this->assertInstanceOf('MongoDB\Driver\WriteConcern', $debug['writeConcern']);
        $this->assertSame(WriteConcern::MAJORITY, $debug['writeConcern']->getW());
    }

    public function testWithOptionsPassesReadPreferenceAndWriteConcern()
    {
        $collectionOptions = [
            'readConcern' => new ReadConcern(ReadConcern::LOCAL),
            'readPreference' => new ReadPreference(ReadPreference::RP_SECONDARY_PREFERRED),
            'writeConcern' => new WriteConcern(WriteConcern::MAJORITY),
        ];

        $clone = $this->collection->withOptions($collectionOptions);
        $debug = $clone->__debugInfo();

        $this->assertInstanceOf('MongoDB\Driver\ReadConcern', $debug['readConcern']);
        $this->assertSame(ReadConcern::LOCAL, $debug['readConcern']->getLevel());
        $this->assertInstanceOf('MongoDB\Driver\ReadPreference', $debug['readPreference']);
        $this->assertSame(ReadPreference::RP_SECONDARY_PREFERRED, $debug['readPreference']->getMode());
        $this->assertInstanceOf('MongoDB\Driver\WriteConcern', $debug['writeConcern']);
        $this->assertSame(WriteConcern::MAJORITY, $debug['writeConcern']->getW());
    }

    /**
     * Create data fixtures.
     *
     * @param integer $n
     */
    private function createFixtures($n)
    {
        $bulkWrite = new BulkWrite(['ordered' => true]);

        for ($i = 1; $i <= $n; $i++) {
            $bulkWrite->insert([
                '_id' => $i,
                'x' => (integer) ($i . $i),
            ]);
        }

        $result = $this->manager->executeBulkWrite($this->getNamespace(), $bulkWrite);

        $this->assertEquals($n, $result->getInsertedCount());
    }
}
