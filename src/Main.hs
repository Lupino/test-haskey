{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE FlexibleContexts   #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE MonoLocalBinds     #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE RankNTypes         #-}
module Main where

import           Control.Lens                     (Lens', lens, (%%~), (^.))
import           Control.Monad.Haskey
import           Control.Monad.IO.Class           (liftIO)
import           Control.Monad.Reader

import           Data.Binary                      (Binary)
import           Data.BTree.Alloc                 (AllocM, AllocReaderM)
import           Data.BTree.Impure                (Tree)
import qualified Data.BTree.Impure                as B
import           Data.BTree.Primitives            (Value)
import           Data.ByteString                  (ByteString)
import qualified Data.ByteString.Char8            as BS (lines, readFile)
import           Data.Maybe                       (fromMaybe)
import           Data.Typeable                    (Typeable)

import           Database.Haskey.Alloc.Concurrent (Root)

import           GHC.Generics                     (Generic)

--------------------------------------------------------------------------------
-- Our application monad transformer stack, includes the HaskeyT monad
-- transformer.
--------------------------------------------------------------------------------

type App a = HaskeyT Schema IO a

--------------------------------------------------------------------------------
-- Definition of our custom schema. As well as query and modify functions.
--------------------------------------------------------------------------------

type MyTree = Tree ByteString ByteString

data Schema = Schema
  { _schemaTrees1 :: Tree ByteString MyTree
  , _schemaTrees2 :: Tree ByteString MyTree
  } deriving (Generic, Show, Typeable)

instance Binary Schema
instance Value Schema
instance Root Schema

emptySchema :: Schema
emptySchema = Schema B.empty B.empty

schemaTrees1 :: Lens' Schema (Tree ByteString MyTree)
schemaTrees1 = lens _schemaTrees1 $ \s x -> s { _schemaTrees1 = x }

schemaTrees2 :: Lens' Schema (Tree ByteString MyTree)
schemaTrees2 = lens _schemaTrees2 $ \s x -> s { _schemaTrees2 = x }

updateTree1
  :: (AllocM n, AllocReaderM n) => (Tree ByteString MyTree -> n (Tree ByteString MyTree)) -> Schema -> n Schema
updateTree1 f = schemaTrees1 %%~ f

updateTree2
  :: (AllocM n, AllocReaderM n) => (Tree ByteString MyTree -> n (Tree ByteString MyTree)) -> Schema -> n Schema
updateTree2 f = schemaTrees2 %%~ f

queryTree1
  :: AllocReaderM n => (Tree ByteString MyTree -> n a) -> Schema -> n a
queryTree1 f root = f (root ^. schemaTrees1)

queryTree2
  :: AllocReaderM n => (Tree ByteString MyTree -> n a) -> Schema -> n a
queryTree2 f root = f (root ^. schemaTrees2)

insertTree
  :: (AllocM n, AllocReaderM n)
  => ByteString -> ByteString -> ByteString -> Tree ByteString MyTree -> n (Tree ByteString MyTree)
insertTree k0 k1 v tree =
  fromMaybe B.empty <$> B.lookup k0 tree
    >>= B.insert k1 v
    >>= flip (B.insert k0) tree

deleteTree
  :: (AllocM n, AllocReaderM n)
  => ByteString -> ByteString -> Tree ByteString MyTree -> n (Tree ByteString MyTree)
deleteTree k0 k1 tree =
  fromMaybe B.empty <$> B.lookup k0 tree
    >>= B.delete k1
    >>= flip (B.insert k0) tree

foldrTree
  :: (AllocReaderM n)
  => (ByteString -> a -> a) -> a -> Tree ByteString MyTree -> n a
foldrTree f = B.foldrM (flip (B.foldr f))

--------------------------------------------------------------------------------
-- Our main application.
--------------------------------------------------------------------------------
main :: IO ()
main = do
  files <- BS.lines <$> BS.readFile "files.txt"
  let hnds = concurrentHandles "test_haskey.db"

  db <- flip runFileStoreT defFileStoreConfig $
    openConcurrentDb hnds >>= \case
        Nothing -> createConcurrentDb hnds emptySchema
        Just db -> return db

  runHaskeyT (app files) db defFileStoreConfig

insertTree1 :: ByteString -> App ()
insertTree1 file =
  transact_ $ updateTree1 (insertTree "test" file file) >=> commit_

insertTree2 :: ByteString -> App ()
insertTree2 file =
  transact_ $ updateTree1 (insertTree "test" file file) >=> commit_

deleteTree1 :: ByteString -> App ()
deleteTree1 file =
  transact_ $ updateTree1 (deleteTree "test" file) >=> commit_

deleteTree2 :: ByteString -> App ()
deleteTree2 file =
  transact_ $ updateTree1 (deleteTree "test" file) >=> commit_

app :: [ByteString] -> App ()
app files = do
  liftIO $ putStrLn "insert file name to tree1"
  mapM_ insertTree1 files
  liftIO $ putStrLn "finish insert file name to tree1"
  mainLoop

mainLoop :: App ()
mainLoop = do
  files <- transactReadOnly $ queryTree1 (foldrTree (foldFunc 100) [])
  liftIO $ putStrLn $ "mainLoop: process file name: " <> show (length files)
  mapM_ processOne files
  if null files then pure ()
                else mainLoop
  where foldFunc :: Int -> ByteString -> [ByteString] -> [ByteString]
        foldFunc n bs xs | length xs > n = xs
                         | otherwise = bs : xs

processOne :: ByteString -> App ()
processOne bs = do
  insertTree2 bs
  deleteTree1 bs
  deleteTree2 bs
