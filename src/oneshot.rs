use std::{
    cell::RefCell,
    future::Future,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

/// Error returned when the sender is dropped before sending.
#[derive(Debug)]
pub struct Canceled;

enum State<T> {
    /// No value yet, and no receiver waker stored.
    Empty,
    /// No value yet, but we have a waker to notify.
    Waiting(Waker),
    /// Value is ready to be taken.
    Ready(T),
    /// Channel is closed (sender dropped without sending).
    Closed,
}

struct Inner<T> {
    state: State<T>,
}

pub struct Sender<T> {
    inner: Rc<RefCell<Inner<T>>>,
}

pub struct Receiver<T> {
    inner: Rc<RefCell<Inner<T>>>,
}

/// Create a one-shot channel.
///
/// `Sender` and `Receiver` are `!Send` and meant for single-threaded runtimes
/// like Glommio.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Rc::new(RefCell::new(Inner {
        state: State::Empty,
    }));
    let tx = Sender {
        inner: inner.clone(),
    };
    let rx = Receiver { inner };
    (tx, rx)
}

impl<T> Sender<T> {
    /// Send the value into the channel.
    ///
    /// Returns `Err(val)` if the receiver was already dropped.
    pub fn send(self, val: T) -> Result<(), T> {
        let mut inner = self.inner.borrow_mut();

        match std::mem::replace(&mut inner.state, State::Closed) {
            State::Empty => {
                inner.state = State::Ready(val);
                Ok(())
            }
            State::Waiting(waker) => {
                inner.state = State::Ready(val);
                waker.wake();
                Ok(())
            }
            State::Ready(_) => {
                panic!("Sender sent value twice - this should be impossible");
            }
            State::Closed => {
                // Receiver dropped.
                Err(val)
            }
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut inner = self.inner.borrow_mut();

        match std::mem::replace(&mut inner.state, State::Closed) {
            State::Waiting(waker) => {
                // Notify the receiver that the channel is closed.
                waker.wake();
            }
            // If it was Ready/Closed/Empty we just leave it as Closed.
            _ => {
                inner.state = State::Closed;
            }
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, Canceled>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.inner.borrow_mut();

        match std::mem::replace(&mut inner.state, State::Empty) {
            State::Ready(value) => {
                // Value is ready, complete the future.
                Poll::Ready(Ok(value))
            }
            State::Closed => {
                // Sender dropped without sending.
                Poll::Ready(Err(Canceled))
            }
            State::Empty | State::Waiting(_) => {
                // Not ready yet; store/replace the waker and return Pending.
                inner.state = State::Waiting(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
