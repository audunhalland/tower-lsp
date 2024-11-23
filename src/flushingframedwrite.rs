use bytes::BytesMut;
use futures::Sink;
use pin_project_lite::pin_project;
use std::task::{ready, Poll};
use tokio::io::AsyncWrite;
use tokio_util::codec::Encoder;

pin_project! {
    /// A FramedWrite (Sink that routes to an AsyncWrite using an Encoder) that flushes the underlying buffer after each item written
    pub struct FlushingFramedWrite<W, C> {
        #[pin]
        write: W,
        encoder: C,
        buf: BytesMut,
        state: State,
    }
}

enum State {
    SinkReceive,
    Write,
    Flush,
}

impl<W, C> FlushingFramedWrite<W, C> {
    pub fn new(write: W, encoder: C) -> Self {
        Self {
            write,
            encoder,
            buf: BytesMut::new(),
            state: State::SinkReceive,
        }
    }
}

impl<I, W, C> Sink<I> for FlushingFramedWrite<W, C>
where
    W: AsyncWrite,
    C: Encoder<I>,
    C::Error: From<tokio::io::Error>,
{
    type Error = C::Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.write_and_flush_item(cx)
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: I) -> Result<(), Self::Error> {
        let mut proj = self.project();

        proj.buf.clear();
        proj.encoder.encode(item, &mut proj.buf)?;
        *proj.state = State::Write;

        Ok(())
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        self.write_and_flush_item(cx)
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        if !self.as_ref().buf.is_empty() {
            ready!(self.as_mut().write_and_flush_item::<C::Error>(cx))?;
        }

        let proj = self.project();
        let _ = ready!(proj.write.poll_shutdown(cx));
        Poll::Ready(Ok(()))
    }
}

impl<W, C> FlushingFramedWrite<W, C>
where
    W: AsyncWrite,
{
    fn write_and_flush_item<E: From<tokio::io::Error>>(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), E>> {
        let mut proj = self.project();

        loop {
            match proj.state {
                State::SinkReceive => {
                    return Poll::Ready(Ok(()));
                }
                State::Write => {
                    let written = ready!(proj.write.as_mut().poll_write(cx, proj.buf))?;
                    let _ = proj.buf.split_to(written);

                    if proj.buf.is_empty() {
                        *proj.state = State::Flush;
                    } else {
                        return Poll::Pending;
                    };
                }
                State::Flush => {
                    ready!(proj.write.as_mut().poll_flush(cx))?;
                    *proj.state = State::SinkReceive;

                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}
