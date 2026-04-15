## part1
  - allocate: increments numWaiting[priority] before releasing mtx, so the count is
   consistent when deallocate inspects it. Returns value (needed by main).      
  - deallocate: takes newValue, checks numWaiting[] instead of the non-existent    
  getValue, and uses the standard mutex-handoff pattern — when waking a waiter via 
  sems[p].notify(), it does not also notify mtx, so the waiter inherits ownership  
  and signals mtx itself.                                                          
  - Priority 1 is still checked before priority 0, preserving the priority         
  scheduling.  

## part2
- allocate: locks mtx, inserts (id, priority) into the queue, then waits on cond 
  until it's at the front — the classic while(not-my-turn) wait() pattern that     
  handles spurious wakes and notifyAll correctly. Returns value under the lock.    
  - deallocate: locks, writes the new value, pops itself off the front, and        
  notifyAlls so every waiter re-checks the queue front. Only the new front's while 
  condition becomes false and proceeds.
                                                                                   
  The PriorityQueue's stable sort gives FCFS within each priority, so same-priority
   users execute in insertion order.  

## part3 
  - allocateHigh: barrier when not busy — high-priority tasks only need to wait for
   the resource to be free.                                                      
  - allocateLow: barrier when not busy and allocateHigh'Count = 0 — low-priority   
  tasks additionally defer whenever any high-priority task is queued. This is the  
  key trick the hint points to: using one entry's 'Count attribute inside another  
  entry's guard gives you priority scheduling for free.                            
  - Both entries set busy := True atomically (protected-object semantics guarantee 
  this) before copying value out.                                                  
  - deallocate writes the new value and clears busy. When busy changes, Ada        
  automatically re-evaluates the barriers, so a waiting allocateHigh gets unblocked
   first; only when none are queued does an allocateLow proceed. 

## part4
  request.go — single request channel + priority queue. The manager keeps busy + a 
  PriorityQueue. Every loop iteration it first checks "can I dispatch now?" (idle  
  and queue non-empty) and hands the resource to the highest-priority waiter. Then 
  the select handles two events: new requests get queued (or served immediately if 
  idle), and giveBack flips busy back to false. Simple and race-free since all     
  state lives in one goroutine.
                                                                                   
  priorityselect.go — two take channels, no state at all. Uses the canonical Go    
  priority trick: an outer select that tries only takeHigh <- res with a default   
  clause. If a high-priority user is already parked on takeHigh, it fires and we   
  skip the fallback. Otherwise the default runs an inner select over both takeHigh 
  and takeLow so a low-priority user can proceed. After each handoff the manager
  synchronously waits on giveBack, which is what enforces mutual exclusion — no
  busy flag needed, as the hint suggested.

## reflections
● 1. How the mechanisms differ                                                     
                                                                                   
  They all block a task until a condition becomes true, but they differ in who     
  tracks what:                                                                     
                                                                                   
  - Semaphores are rawest — the runtime only tracks a single int. Everything else  
  (priority, "busy") you encode yourself, which is why my fix needed numWaiting[2],
   a busy flag, and a mutex-handoff dance.                                         
  - Condition variables add atomic unlock-and-wait, but the predicate is still     
  yours — hence the canonical while(!cond) wait().
  - Java monitors are CVs fused into the object (one implicit mutex + wait queue). 
  Ergonomic but strictly less flexible than explicit Conditions.
  - Ada protected objects are the most declarative. You write the predicate once as
   a when barrier and the runtime re-evaluates it. 'Count on one entry can appear
  in another's barrier, which is exactly what made the priority rule a one-liner.  
                  
  Spectrum: semaphore → CV → monitor → protected object, each step moving          
  bookkeeping out of your code and into the runtime.
                                                                                   
  2. Confidence and code quality                                                   
                                          
  Ranked most → least confident:                                                   
                                                                                   
  1. Ada protected object — the priority rule is two barrier expressions; there's
  almost nowhere for a bug to hide.                                                
  2. Condition variable — while(queue.front != id) wait() + notifyAll is the
  canonical pattern.                                                               
  3. Message-passing (request.go) — all mutable state in one goroutine, no races by
   construction.                                                                   
  4. Message-passing (priorityselect.go) — clever, but the default: + nested select
   has edge cases.                            
  5. Semaphore — correct but fragile; the mutex-handoff protocol (deallocate       
  signals sems[p] but not mtx) breaks under any innocent-looking refactor.
                                                                                   
  The lesson: quality is inversely proportional to how much invariant you have to 
  hold in your head to read the code. Code that offloads invariants to the runtime 
  is obviously correct, and in concurrent code that's worth paying for — the bugs
  you don't catch lurk for months and corrupt data under load.                     
                  
  3. Extending to N priorities                                                     
                  
  - Ada: use an entry family — entry allocate(Priority_Range) with a barrier that
  checks higher-priority counts.          
  - CV + priority queue and request.go: zero code changes. Both already accept any
  int priority.                                                                    
  - Semaphore: grows linearly and ugly — Semaphore[N], int[N], deallocate cascade.
  Every new level is a new place the handoff can drift.                            
  - priorityselect.go: does not generalize elegantly. You'd need N nested selects, 
  each with a default falling into a less-selective one. Unreadable by N=4.
                                                                                   
  The lesson: a solution that works at N=2 is not the same as a solution. The
  priority-select trick only looks elegant because binary priority fits in one     
  default. Any design that encodes priority through channel structure instead of
  data hits this wall.                                                             
                  
  4. getValue on semaphores                                                        
                  
  Essentially never appropriate. Two reasons: 
                                          
  Practical: any value you read is immediately stale. My original buggy code used
  GetValue(PS[p]) < 0 to decide whether a waiter existed — the answer can change   
  before the next instruction, so the check is racy without an external mutex.
                                                                                   
  Conceptual: a semaphore is a synchronization object, not a data object. Its
  counter is implementation scaffolding, meaningful only at the atomic moment      
  wait/signal evaluate it. Exposing getValue invites you to treat it as shared data
   — a category error. The fact that my fix had to introduce a separate            
  numWaiting[2] under my own mutex proves the point: the count I needed was part of
   my invariant, not the semaphore's.                                              
                  
  This is why Windows omits it, POSIX calls sem_getvalue unreliable for
  synchronization, and D's stdlib just leaves it out.

  5. Preference                                                                    
   
  For this specific task, the Ada protected object wins hands-down — shortest      
  correct solution, priority rule stated exactly once, exactly where you look.

  In general, I'd reach for message-passing with an owner goroutine (request.go    
  style). Not prettier, but it composes: protected objects are great until you need
   to wait on two resources, add timeouts, or combine with I/O — at which point the
   barriers don't compose and you fall off a cliff. A goroutine with select handles
   cancellation, timeouts, and multi-resource coordination naturally. CVs I'd keep
  for hot paths where channel overhead matters. Semaphores I'd essentially never   
  write directly — they're the assembly language of synchronization: useful to
  understand, not to live in.

