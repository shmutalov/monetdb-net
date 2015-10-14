/*
 * The contents of this file are subject to the MonetDB Public License
 * Version 1.1 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 * http://monetdb.cwi.nl/Legal/MonetDBLicense-1.1.html
 * 
 * Software distributed under the License is distributed on an "AS IS"
 * basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
 * License for the specific language governing rights and limitations
 * under the License.
 * 
 * The Original Code is "A .NET Blocking Queue Class".
 * 
 * The Initial Developer of the Original Code is Daniel Schwieg.
 * Portions created by Daniel Schwieg are Copyright (C) 2007. All Rights Reserved.
 * 
 * Contributor(s): Tim Gebhardt<tim@gebhardtcomputing.com>.
 */

/*
 * The original article detailing the creation of this class can be found at the 
 * following link:
 * 
 * http://www.eggheadcafe.com/articles/20060414.asp
 */

using System;
using System.Collections;
using System.Threading;

namespace System.Collections.Generic
{
    /// <summary>
    /// Same as Queue except Dequeue function blocks until there is an object to return.
    /// Note: This class does not need to be synchronized
    /// </summary>
    internal class BlockingQueue<T> : Queue<T>
    {
        private bool open;
        private object SyncRoot = new object();

        /// <summary>
        /// Create new BlockingQueue.
        /// </summary>
        /// <param name="col">The System.Collections.Generic.ICollection&lt;T&gt; to copy elements from</param>
        public BlockingQueue(ICollection<T> col)
            : base(col)
        {
            open = true;
        }

        /// <summary>
        /// Create new BlockingQueue.
        /// </summary>
        /// <param name="capacity">The initial number of elements that the queue can contain</param>
        public BlockingQueue(int capacity)
            : base(capacity)
        {
            open = true;
        }

        /// <summary>
        /// Create new BlockingQueue.
        /// </summary>
        public BlockingQueue()
            : base()
        {
            open = true;
        }

        /// <summary>
        /// BlockingQueue Destructor (Close queue, resume any waiting thread).
        /// </summary>
        ~BlockingQueue()
        {
            Close();
        }

        /// <summary>
        /// Remove all objects from the Queue.
        /// </summary>
        public new void Clear()
        {
            lock (SyncRoot)
            {
                base.Clear();
            }
        }

        /// <summary>
        /// Remove all objects from the Queue, resume all dequeue threads.
        /// </summary>
        public void Close()
        {
            lock (SyncRoot)
            {
                open = false;
                base.Clear();
                Monitor.PulseAll(SyncRoot);    // resume any waiting threads
            }
        }

        /// <summary>
        /// Removes and returns the object at the beginning of the Queue.
        /// </summary>
        /// <returns>Object in queue.</returns>
        public new T Dequeue()
        {
            return Dequeue(Timeout.Infinite);
        }

        /// <summary>
        /// Removes and returns the object at the beginning of the Queue.
        /// </summary>
        /// <param name="timeout">time to wait before returning</param>
        /// <returns>Object in queue.</returns>
        public T Dequeue(TimeSpan timeout)
        {
            return Dequeue(timeout.Milliseconds);
        }

        /// <summary>
        /// Removes and returns the object at the beginning of the Queue.
        /// </summary>
        /// <param name="timeout">time to wait before returning (in milliseconds)</param>
        /// <returns>Object in queue.</returns>
        public T Dequeue(int timeout)
        {
            lock (SyncRoot)
            {
                while (open && (base.Count == 0))
                {
                    if (!Monitor.Wait(SyncRoot, timeout))
                        throw new InvalidOperationException("Timeout");
                }
                if (open)
                    return base.Dequeue();
                else
                    throw new InvalidOperationException("Queue Closed");
            }
        }

        /// <summary>
        /// Adds an object to the end of the Queue.
        /// </summary>
        /// <param name="obj">Object to put in queue</param>
        public new void Enqueue(T obj)
        {
            lock (SyncRoot)
            {
                base.Enqueue(obj);
                Monitor.Pulse(SyncRoot);
            }
        }

        /// <summary>
        /// Open Queue.
        /// </summary>
        public void Open()
        {
            lock (SyncRoot)
            {
                open = true;
            }
        }

        /// <summary>
        /// Gets flag indicating if queue has been closed.
        /// </summary>
        public bool Closed
        {
            get
            {
                return !open;
            }
        }
    }
}
