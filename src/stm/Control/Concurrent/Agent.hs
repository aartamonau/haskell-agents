module Agent (Agent,
              Agent.create, Agent.read, Agent.send, Agent.await, Agent.awaitFor,
              Agent.isReady, Agent.restart)
       where

import Control.Concurrent (ThreadId, forkIO, killThread)
import Control.Concurrent.STM (STM, atomically, retry,
                               TVar, newTVar, readTVar, writeTVar,
                               TMVar, newTMVar, newEmptyTMVar,
                                      takeTMVar, putTMVar)
import Control.Monad (unless, when)

import Control.Parallel (pseq)
import Control.Parallel.Strategies (NFData (..))

import Data.Maybe (isJust, fromJust)
import Data.Sequence (Seq, ViewL (..), (|>))
import qualified Data.Sequence as Seq

import System.Timeout (timeout)


type AgentTask s = s -> s

data Agent s =
  Agent { state  :: TVar s
        , pool   :: TVar (Seq (AgentTask s))
        , worker :: TMVar (Maybe ThreadId) }

create :: NFData s => s -> IO (Agent s)
create v = do
  agent <- atomically $ do
    state  <- newTVar v
    pool   <- newTVar Seq.empty
    worker <- newEmptyTMVar

    return $ Agent state pool worker

  tid <- forkIO (executor agent)
  atomically $ putTMVar (worker agent) (Just tid)

  return agent

executor :: NFData s => Agent s -> IO ()
executor a@(Agent state pool _) = do
  task <- atomically $ do
    isEmpty <- fmap Seq.null (readTVar pool)
    when isEmpty retry

    task :< _ <- fmap Seq.viewl (readTVar pool)
    return task

  curState <- atomically $ readTVar state

  -- The state can be changed only by executor or restart functions
  -- In the first case we can safely split read and write to different
  -- transactions.
  -- In the second case the thread running executor function is explicitly
  -- terminated beforehand so the agent won't be in the wrong state.
  let newState = task curState
  rnf newState `pseq` atomically $ writeTVar state newState

  atomically $ do
    -- Only executor and restart can decrease a length of the pool.
    -- As stated above the second case is explicitly handled by restart itself.
    --
    -- Hence the following pattern matching is correct.
    _ :< tasks <- fmap Seq.viewl (readTVar pool)
    writeTVar pool tasks

  executor a

read :: Agent s -> IO s
read = atomically . readTVar . state

send :: Agent s -> AgentTask s -> IO ()
send (Agent _ pool _) task =
  atomically $ do
    pool' <- readTVar pool
    writeTVar pool (pool' |> task)

await :: Agent s -> IO s
await (Agent state pool _) =
  atomically $ do
    isEmpty <- fmap Seq.null (readTVar pool)
    unless (isEmpty) retry

    readTVar state

awaitFor :: Int -> Agent s -> IO (Maybe s)
awaitFor n = timeout n . await

isReady :: Agent s -> IO Bool
isReady = (fmap Seq.null) . atomically . readTVar . pool

restart :: NFData s => Agent s -> s -> IO ()
restart agent@(Agent state pool worker) value = do
  -- avoiding cocurrent executions of restart
  tid <- atomically $ takeTMVar worker

  when (isJust tid) $ killThread (fromJust tid)

  -- concurrent calls to other functions are OK
  atomically $ do
    writeTVar state value
    writeTVar pool Seq.empty

  tid <- forkIO (executor agent)

  atomically $ putTMVar worker (Just tid)
