<?php

namespace MongoDB\Operation;


use MongoDB\BSON\Javascript;
use MongoDB\Driver\Command;
use MongoDB\Driver\Server;
use \ArrayIterator;
use \Traversable;
use MongoDB\Exception\InvalidArgumentTypeException;
use MongoDB\Exception\RuntimeException;
use MongoDB\Exception\UnexpectedValueException;

class Group implements Executable
{
    public $collectionName;
    public $databaseName;
    public $keys;
    public $initial;
    public $reduce;
    public $options;

    /**
     * Group constructor.
     * @param string $databaseName
     * @param string $collectionName The collection from which to perform the group by operation.
     * @param mixed $keys Fields to group by. If an array or non-code object is passed, it will be the key used to group results.
     * @param array $initial Initializes the aggregation result document.
     * @param array $options Other optional fields
     * @param Javascript $reduce An aggregation function that operates on the documents during the grouping operation.
     */
    public function __construct($databaseName, $collectionName, $keys, array $initial, Javascript $reduce, array $options = [])
    {

        if (isset($options['finalize']) && !$options['finalize'] instanceof Javascript) {
            throw new InvalidArgumentTypeException('"finalize" option', $options['finalize'], 'MongoDB\BSON\Javascript');
        }

        if (isset($options['cond']) && !is_array($options['cond']) && !is_object($options['cond'])) {
            throw new InvalidArgumentTypeException('"cond" option', $options['cond'], 'array or object');
        }

        if (!is_array($keys) && !is_object($keys) && !$keys instanceof Javascript) {
            throw new InvalidArgumentTypeException('"keys" option', $keys, 'array, object or instance of MongoDB\BSON\Javascript');
        }

        $this->collectionName = (string) $collectionName;
        $this->databaseName = (string) $databaseName;
        $this->keys = $keys;
        $this->initial = (object) $initial;
        $this->reduce = $reduce;
        $this->options = $options;
    }

    /**
     * Execute the operation.
     *
     * @see Executable::execute()
     * @param Server $server
     * @return Traversable
     */
    public function execute(Server $server)
    {
        $key = $this->keys instanceof Javascript ? '$keyf' : 'key';
        $cmd = [
            'group' => [
                $key => $this->keys,
                'ns' => $this->collectionName,
                '$reduce' => $this->reduce,
                'initial' => $this->initial
            ] + $this->options
        ];

        $cursor = $server->executeCommand($this->databaseName, new Command($cmd));
        $result = current($cursor->toArray());

        if (empty($result->ok)) {
            throw new RuntimeException(isset($result->errmsg) ? $result->errmsg : 'Unknown error');
        }

        if ( ! isset($result->retval) || ! is_array($result->retval)) {
            throw new UnexpectedValueException('group command did not return a "retval" array');
        }
        return new ArrayIterator($result->retval);

    }
}