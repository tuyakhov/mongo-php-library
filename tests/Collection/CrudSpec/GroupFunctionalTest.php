<?php

namespace MongoDB\Tests\Collection\CrudSpec;

use MongoDB\BSON\Javascript;
use MongoDB\Driver\BulkWrite;

/**
 * CRUD spec functional tests for group().
 *
 * @see https://github.com/mongodb/specifications/tree/master/source/crud/tests
 */
class GroupFunctionalTest extends FunctionalTestCase
{
    public function setUp()
    {
        parent::setUp();

        $this->createFixtures(3);

        $bulkWrite = new BulkWrite(['ordered' => true]);

        $bulkWrite->insert([
            '_id' => 4,
            'x' => 11,
        ]);
        $this->manager->executeBulkWrite($this->getNamespace(), $bulkWrite);
    }

    public function testGroup()
    {
        $reduce = new Javascript("function (curr, result) {result.total++}");

        $result = $this->collection->group(['x' => 1], ['total' => 0], $reduce, ['cond' => ['x' => ['$lt' => 22]]]);

        $expected = [
            ['x' => 11, 'total' => 2],
        ];

        $this->assertSameDocuments($expected, $result);
    }

    public function testGroupWithKeyFunction()
    {
        $keyf = new Javascript("function(doc) { return {calculated: doc.x} }");
        $reduce = new Javascript("function (curr, result) {}");

        $result = $this->collection->group($keyf, [], $reduce, ['cond' => ['x' => ['$lt' => 22]]]);

        $expected = [
            ['calculated' => 11],
        ];

        $this->assertSameDocuments($expected, $result);
    }
}
