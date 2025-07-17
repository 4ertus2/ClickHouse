#include <Common/MemoryTracker.h>
#include <Common/CurrentThread.h>

#include <Common/CurrentMemoryTracker.h>


#ifdef MEMORY_TRACKER_DEBUG_CHECKS
thread_local bool memory_tracker_always_throw_logical_error_on_allocation = false;
#endif

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

namespace
{

MemoryTracker * getMemoryTracker()
{
    if (auto * thread_memory_tracker = DB::CurrentThread::getMemoryTracker())
        return thread_memory_tracker;

    /// Once the main thread is initialized,
    /// total_memory_tracker is initialized too.
    /// And can be used, since MainThreadStatus is required for profiling.
    if (DB::MainThreadStatus::get())
        return &total_memory_tracker;

    return nullptr;
}

}

using DB::current_thread;

template <bool throw_if_memory_exceeded>
AllocationTrace CurrentMemoryTracker::allocImpl(Int64 size)
{
#ifdef MEMORY_TRACKER_DEBUG_CHECKS
    if (unlikely(memory_tracker_always_throw_logical_error_on_allocation))
    {
        memory_tracker_always_throw_logical_error_on_allocation = false;
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Memory tracker: allocations not allowed.");
    }
#endif

    if (auto * memory_tracker = getMemoryTracker())
    {
        if (current_thread)
        {
            Int64 previous_untracked_memory = current_thread->untracked_memory;
            current_thread->untracked_memory += size;
            if (current_thread->untracked_memory > current_thread->untracked_memory_limit)
            {
                Int64 current_untracked_memory = current_thread->untracked_memory;
                current_thread->untracked_memory = 0;

                try
                {
                    return memory_tracker->allocImpl<throw_if_memory_exceeded>(current_untracked_memory);
                }
                catch (...)
                {
                    current_thread->untracked_memory += previous_untracked_memory;
                    throw;
                }
            }
        }
        /// total_memory_tracker only, ignore untracked_memory
        else
        {
            return memory_tracker->allocImpl<throw_if_memory_exceeded>(size);
        }

        return AllocationTrace(memory_tracker->getSampleProbability(size));
    }

    return AllocationTrace(0);
}

template AllocationTrace CurrentMemoryTracker::allocImpl<false>(Int64 size);
template AllocationTrace CurrentMemoryTracker::allocImpl<true>(Int64 size);

void CurrentMemoryTracker::check()
{
    constexpr bool throw_if_memory_exceeded = true;
    if (auto * memory_tracker = getMemoryTracker())
        std::ignore = memory_tracker->allocImpl<throw_if_memory_exceeded>(0);
}

AllocationTrace CurrentMemoryTracker::alloc(Int64 size)
{
    constexpr bool throw_if_memory_exceeded = true;
    return allocImpl<throw_if_memory_exceeded>(size);
}

AllocationTrace CurrentMemoryTracker::allocNoThrow(Int64 size)
{
    constexpr bool throw_if_memory_exceeded = false;
    return allocImpl<throw_if_memory_exceeded>(size);
}

AllocationTrace CurrentMemoryTracker::free(Int64 size)
{
    if (auto * memory_tracker = getMemoryTracker())
    {
        if (current_thread)
        {
            current_thread->untracked_memory -= size;
            if (current_thread->untracked_memory < -current_thread->untracked_memory_limit)
            {
                Int64 untracked_memory = current_thread->untracked_memory;
                current_thread->untracked_memory = 0;
                return memory_tracker->free(-untracked_memory);
            }
        }
        /// total_memory_tracker only, ignore untracked_memory
        else
        {
            return memory_tracker->free(size);
        }

        return AllocationTrace(memory_tracker->getSampleProbability(size));
    }

    return AllocationTrace(0);
}

void CurrentMemoryTracker::injectFault()
{
    if (auto * memory_tracker = getMemoryTracker())
        memory_tracker->injectFault();
}

