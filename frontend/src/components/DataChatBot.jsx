import { useState, useRef, useEffect } from "react";
import { MessageCircle, X, Send, Loader2, User, Bot } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import axios from "axios";

import { API } from "@/apiConfig";

export default function DataChatBot() {
  const [open, setOpen] = useState(false);
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const scrollRef = useRef(null);

  const scrollToBottom = () => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const sendMessage = async () => {
    const text = (input || "").trim();
    if (!text || loading) return;

    setInput("");
    const userMsg = { role: "user", content: text };
    setMessages((prev) => [...prev, userMsg]);
    setLoading(true);

    try {
      const { data } = await axios.post(
        `${API}/chat`,
        { message: text },
        { timeout: 60000 }
      );
      const answer = data?.answer ?? "No response from the assistant.";
      setMessages((prev) => [...prev, { role: "assistant", content: answer }]);
    } catch (err) {
      const detail = err.response?.data?.detail ?? err.message ?? "Failed to get an answer.";
      setMessages((prev) => [
        ...prev,
        { role: "assistant", content: `Sorry, I couldn't answer that: ${detail}` },
      ]);
    } finally {
      setLoading(false);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      sendMessage();
    }
  };

  return (
    <>
      {/* Floating chat button */}
      <button
        onClick={() => setOpen(true)}
        className="fixed right-6 bottom-6 w-14 h-14 rounded-full bg-gradient-to-r from-[#D63384] to-purple-600 hover:from-[#C2185B] hover:to-purple-700 text-white shadow-lg flex items-center justify-center z-40 transition-transform hover:scale-105"
        aria-label="Open data chat"
        data-testid="chat-bot-toggle"
      >
        <MessageCircle className="w-6 h-6" />
      </button>

      {/* Chat panel */}
      {open && (
        <div
          className="fixed right-6 bottom-24 w-[420px] max-w-[calc(100vw-3rem)] bg-white/95 backdrop-blur-xl border border-[#D63384]/20 shadow-2xl rounded-2xl z-50 overflow-hidden flex flex-col h-[520px] animate-fade-in"
          data-testid="chat-bot-panel"
        >
          <div className="chat-panel-header px-4 py-3 text-white flex justify-between items-center shrink-0">
            <div className="flex items-center gap-2">
              <MessageCircle className="w-5 h-5" />
              <span className="font-semibold font-['Manrope']">Data & Insights Chat</span>
            </div>
            <button
              onClick={() => setOpen(false)}
              className="p-1.5 hover:bg-white/20 rounded-lg transition-colors"
              aria-label="Close chat"
            >
              <X className="w-4 h-4" />
            </button>
          </div>

          <div
            ref={scrollRef}
            className="flex-1 p-4 overflow-y-auto min-h-0"
            style={{ maxHeight: "360px" }}
          >
            {messages.length === 0 && (
              <div className="text-center py-8 text-slate-500 text-sm">
                <Bot className="w-10 h-10 mx-auto mb-3 text-[#D63384]/60" />
                <p className="font-['Manrope'] font-medium text-slate-700">Ask anything about your sales data</p>
                <p className="mt-1 text-slate-400">e.g. What is total net sales? Top products? Customer count? Trends?</p>
              </div>
            )}
            <div className="space-y-4">
              {messages.map((msg, idx) => (
                <div
                  key={idx}
                  className={`flex gap-3 ${msg.role === "user" ? "flex-row-reverse" : ""}`}
                >
                  <div
                    className={`w-8 h-8 rounded-full flex items-center justify-center shrink-0 ${
                      msg.role === "user"
                        ? "bg-slate-200 text-slate-700"
                        : "bg-gradient-to-br from-[#D63384]/20 to-purple-500/20 text-[#D63384]"
                    }`}
                  >
                    {msg.role === "user" ? <User className="w-4 h-4" /> : <Bot className="w-4 h-4" />}
                  </div>
                  <div
                    className={`max-w-[85%] rounded-2xl px-4 py-2.5 text-sm ${
                      msg.role === "user"
                        ? "bg-gradient-to-r from-[#D63384] to-purple-600 text-white"
                        : "bg-slate-100 text-slate-800 border border-slate-200"
                    }`}
                  >
                    <p className="whitespace-pre-wrap">
                      {msg.role === "assistant" && typeof msg.content === "string" && msg.content.includes("**")
                        ? msg.content.split(/(\*\*[^*]+\*\*)/g).map((part, i) =>
                            part.startsWith("**") && part.endsWith("**") ? (
                              <strong key={i}>{part.slice(2, -2)}</strong>
                            ) : (
                              part
                            )
                          )
                        : msg.content}
                    </p>
                  </div>
                </div>
              ))}
              {loading && (
                <div className="flex gap-3">
                  <div className="w-8 h-8 rounded-full bg-gradient-to-br from-[#D63384]/20 to-purple-500/20 flex items-center justify-center shrink-0">
                    <Bot className="w-4 h-4 text-[#D63384]" />
                  </div>
                  <div className="bg-slate-100 rounded-2xl px-4 py-2.5 flex items-center gap-2 text-slate-500 text-sm">
                    <Loader2 className="w-4 h-4 animate-spin" />
                    <span>Thinking...</span>
                  </div>
                </div>
              )}
            </div>
          </div>

          <div className="p-3 border-t border-slate-200 bg-slate-50 shrink-0 flex gap-2">
            <Input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Ask about data or insights..."
              className="flex-1 rounded-xl border-slate-200 bg-white"
              disabled={loading}
            />
            <Button
              onClick={sendMessage}
              disabled={loading || !input.trim()}
              className="rounded-xl bg-gradient-to-r from-[#D63384] to-purple-600 hover:from-[#C2185B] hover:to-purple-700 text-white shrink-0"
            >
              <Send className="w-4 h-4" />
            </Button>
          </div>
        </div>
      )}
    </>
  );
}
