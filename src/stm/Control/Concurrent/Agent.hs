module Agent
       where

import Control.Applicative (Applicative (..), (<$>))

import Control.Concurrent (ThreadId, forkIO, killThread)
import Control.Concurrent.STM (STM, atomically, retry,
                               TVar, newTVar, readTVar, writeTVar,
                               TMVar, newTMVar, takeTMVar, putTMVar)
import Control.Monad (ap, unless, when)

import Control.Parallel (pseq)
import Control.Parallel.Strategies (NFData (..), using)

import Data.Maybe (isJust, fromJust)
import Data.Sequence (Seq, ViewL (..), (|>))
import qualified Data.Sequence as Seq

import System.Timeout (timeout)


instance Applicative STM where
  pure  = return
  (<*>) = ap

type AgentTask s = s -> s

data Agent s =
  Agent { state     :: TVar s
        , pool      :: TVar (Seq (AgentTask s))
        , worker    :: TVar (Maybe ThreadId)
        , exclusion :: TMVar () }

create :: NFData s => s -> IO (Agent s)
create v = do
  agent <- atomically $
             Agent <$> newTVar v
                   <*> newTVar Seq.empty
                   <*> newTVar Nothing
                   <*> newTMVar ()

  tid <- forkIO (executor agent)
  atomically $ writeTVar (worker agent) (Just tid)

  return agent

executor :: NFData s => Agent s -> IO ()
executor a@(Agent state pool _ _) = do
  task <- atomically $ do
    isEmpty <- Seq.null <$> readTVar pool
    when isEmpty retry

    task :< tasks <- Seq.viewl <$> readTVar pool
    writeTVar pool tasks

    state' <- task <$> readTVar state
    rnf state' `pseq` writeTVar state state'

  executor a

read :: Agent s -> IO s
read = atomically . readTVar . state

send :: Agent s -> AgentTask s -> IO ()
send (Agent _ pool _ _) task =
  atomically $ do
    pool' <- readTVar pool
    writeTVar pool (pool' |> task)

await :: Agent s -> IO s
await (Agent state pool _ _) =
  atomically $ do
    isEmpty <- Seq.null <$> readTVar pool
    unless (isEmpty) retry

    readTVar state

awaitFor :: Int -> Agent s -> IO (Maybe s)
awaitFor n = timeout n . await

isReady :: Agent s -> IO Bool
isReady = (fmap Seq.null) . atomically . readTVar . pool

restart :: NFData s => Agent s -> s -> IO ()
restart agent@(Agent state pool worker exclusion) val = do
  -- avoiding cocurrent executions of restart
  atomically $ takeTMVar exclusion

  tid <- atomically $ readTVar worker
  when (isJust tid) $ killThread (fromJust tid)

  -- concurrent calls to other functions are OK
  atomically $ do
    writeTVar state val
    writeTVar pool Seq.empty

  tid <- forkIO (executor agent)
  atomically $ writeTVar worker (Just tid)

  atomically $ putTMVar exclusion ()
