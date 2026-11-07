


#ifndef __ctpl_stl_thread_pool_H__
#define __ctpl_stl_thread_pool_H__

#include <functional>
#include <thread>
#include <atomic>
#include <vector>
#include <memory>
#include <exception>
#include <future>
#include <mutex>
#include <queue>




namespace ctpl {
    namespace detail {
        template<typename T>
        class Queue {
        public:
            bool push(T const &value) {
                std::unique_lock<std::mutex> lock(this->mutex);
                this->q.push(value);
                return true;
            }

            bool pop(T &v) {
                std::unique_lock<std::mutex> lock(this->mutex);
                if (this->q.empty())
                    return false;
                v = this->q.front();
                this->q.pop();
                return true;
            }

            bool empty() {
                std::unique_lock<std::mutex> lock(this->mutex);
                return this->q.empty();
            }

        private:
            std::queue<T> q;
            std::mutex mutex;
        };
    }

    class thread_pool {
    public:
        thread_pool() { this->init(); }

        thread_pool(int nThreads) {
            this->init();
            this->resize(nThreads);
        }

        ~thread_pool() {
            this->stop(true);
        }

        int size() { return static_cast<int>(this->threads.size()); }

        int n_idle() { return this->nWaiting; }
        std::thread &get_thread(int i) { return *this->threads[i]; }

        void resize(int nThreads) {
            if (!this->isStop && !this->isDone) {
                int oldNThreads = static_cast<int>(this->threads.size());
                if (oldNThreads <= nThreads) {
                    this->threads.resize(nThreads);
                    this->flags.resize(nThreads);

                    for (int i = oldNThreads; i < nThreads; ++i) {
                        this->flags[i] = std::make_shared<std::atomic<bool> >(false);
                        this->set_thread(i);
                    }
                } else {
                    for (int i = oldNThreads - 1; i >= nThreads; --i) {
                        *this->flags[i] = true;
                        this->threads[i]->detach();
                    } {
                        std::unique_lock<std::mutex> lock(this->mutex);
                        this->cv.notify_all();
                    }
                    this->threads.resize(nThreads);
                    this->flags.resize(nThreads);
                }
            }
        }

        void clear_queue() {
            std::function<void(int id)> *_f;
            while (this->q.pop(_f))
                delete _f;
        }

        std::function<void(int)> pop() {
            std::function<void(int id)> *_f = nullptr;
            this->q.pop(_f);
            std::unique_ptr<std::function<void(int id)> > func(_f);
            std::function<void(int)> f;
            if (_f)
                f = *_f;
            return f;
        }

        void stop(bool isWait = false) {
            if (!isWait) {
                if (this->isStop)
                    return;
                this->isStop = true;
                for (int i = 0, n = this->size(); i < n; ++i) {
                    *this->flags[i] = true;
                }
                this->clear_queue();
            } else {
                if (this->isDone || this->isStop)
                    return;
                this->isDone = true;
            } {
                std::unique_lock<std::mutex> lock(this->mutex);
                this->cv.notify_all();
            }
            for (int i = 0; i < static_cast<int>(this->threads.size()); ++i) {
                if (this->threads[i]->joinable())
                    this->threads[i]->join();
            }
            this->clear_queue();
            this->threads.clear();
            this->flags.clear();
        }

        template<typename F, typename... Rest>
        auto push(F &&f, Rest &&... rest) -> std::future<decltype(f(0, rest...))> {
            auto pck = std::make_shared<std::packaged_task<decltype(f(0, rest...))(int)> >(
                std::bind(std::forward<F>(f), std::placeholders::_1, std::forward<Rest>(rest)...)
            );
            auto _f = new std::function<void(int id)>([pck](int id) {
                (*pck)(id);
            });
            this->q.push(_f);
            std::unique_lock<std::mutex> lock(this->mutex);
            this->cv.notify_one();
            return pck->get_future();
        }

        template<typename F>
        auto push(F &&f) -> std::future<decltype(f(0))> {
            auto pck = std::make_shared<std::packaged_task<decltype(f(0))(int)> >(std::forward<F>(f));
            auto _f = new std::function<void(int id)>([pck](int id) {
                (*pck)(id);
            });
            this->q.push(_f);
            std::unique_lock<std::mutex> lock(this->mutex);
            this->cv.notify_one();
            return pck->get_future();
        }

    private:
        thread_pool(const thread_pool &);
        thread_pool(thread_pool &&);
        thread_pool &operator=(const thread_pool &);
        thread_pool &operator=(thread_pool &&);

        void set_thread(int i) {
            std::shared_ptr<std::atomic<bool> > flag(this->flags[i]);
            auto f = [this, i, flag]() {
                std::atomic<bool> &_flag = *flag;
                std::function<void(int id)> *_f;
                bool isPop = this->q.pop(_f);
                while (true) {
                    while (isPop) {
                        std::unique_ptr<std::function<void(int id)> > func(_f);
                        (*_f)(i);
                        if (_flag)
                            return;
                        else
                            isPop = this->q.pop(_f);
                    }
                    std::unique_lock<std::mutex> lock(this->mutex);
                    ++this->nWaiting;
                    this->cv.wait(lock, [this, &_f, &isPop, &_flag]() {
                        isPop = this->q.pop(_f);
                        return isPop || this->isDone || _flag;
                    });
                    --this->nWaiting;
                    if (!isPop)
                        return;
                }
            };
            this->threads[i].reset(new std::thread(f));
        }

        void init() {
            this->nWaiting = 0;
            this->isStop = false;
            this->isDone = false;
        }

        std::vector<std::unique_ptr<std::thread> > threads;
        std::vector<std::shared_ptr<std::atomic<bool> > > flags;
        detail::Queue<std::function<void(int id)> *> q;
        std::atomic<bool> isDone;
        std::atomic<bool> isStop;
        std::atomic<int> nWaiting;

        std::mutex mutex;
        std::condition_variable cv;
    };
}

#endif
