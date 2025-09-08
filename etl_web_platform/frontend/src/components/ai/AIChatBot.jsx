import React, { useState, useEffect, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { HiChatBubbleLeftRight, HiPaperAirplane, HiLightBulb, HiArrowPath, HiXMark } from 'react-icons/hi2';

const AIChatBot = () => {
  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState([]);
  const [inputMessage, setInputMessage] = useState('');
  const [isTyping, setIsTyping] = useState(false);
  const [suggestions, setSuggestions] = useState([]);
  const [sessionId] = useState(`session_${Date.now()}`);
  const messagesEndRef = useRef(null);
  const inputRef = useRef(null);

  // Welcome message
  useEffect(() => {
    if (isOpen && messages.length === 0) {
      setMessages([
        {
          id: 'welcome',
          type: 'ai',
          content: `🏦 **Welcome to BVMT Expert Assistant!**

I'm your specialized AI financial analyst for the Tunisian Stock Exchange (BVMT).

💡 **What I can help with:**
• **Company Information**: Details about BVMT listed companies
• **Market News**: Latest announcements and updates  
• **Sector Analysis**: Performance by industry in Tunisia
• **Regulatory Updates**: Market rules and changes
• **Investment Insights**: Market trends and opportunities

**Try asking:**
• "What is BVMT?"
• "Tell me about Tunisian companies"
• "Latest BVMT news"
• "Which sectors perform well in Tunisia?"

I'm here to help you understand the Tunisian financial markets! 🚀`,
          timestamp: new Date().toISOString()
        }
      ]);
      loadSuggestions();
    }
  }, [isOpen]);

  // Auto-scroll to bottom
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  // Load suggested questions
  const loadSuggestions = async () => {
    try {
      const response = await fetch('http://127.0.0.1:5000/api/ai/suggestions');
      if (response.ok) {
        const data = await response.json();
        setSuggestions(data.suggestions);
      }
    } catch (error) {
      console.log('Could not load suggestions:', error);
    }
  };

  // Send message to AI
  const sendMessage = async (message = inputMessage) => {
    if (!message.trim()) return;

    const userMessage = {
      id: Date.now(),
      type: 'user',
      content: message,
      timestamp: new Date().toISOString()
    };

    setMessages(prev => [...prev, userMessage]);
    setInputMessage('');
    setIsTyping(true);

    try {
      const response = await fetch('http://127.0.0.1:5000/api/ai/chat', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          message: message,
          session_id: sessionId
        }),
      });

      if (response.ok) {
        const data = await response.json();
        const aiMessage = {
          id: Date.now() + 1,
          type: 'ai',
          content: data.response,
          timestamp: data.timestamp
        };
        setMessages(prev => [...prev, aiMessage]);
      } else {
        throw new Error('Failed to get AI response');
      }
    } catch (error) {
      console.error('Error sending message:', error);
      const errorMessage = {
        id: Date.now() + 1,
        type: 'ai',
        content: '❌ Sorry, I encountered an error. Please try again or check your connection.',
        timestamp: new Date().toISOString()
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsTyping(false);
    }
  };

  // Handle form submission
  const handleSubmit = (e) => {
    e.preventDefault();
    sendMessage();
  };

  // Handle suggestion click
  const handleSuggestionClick = (suggestion) => {
    setInputMessage(suggestion);
    inputRef.current?.focus();
  };

  // Clear chat history
  const clearChat = () => {
    setMessages([]);
    setIsTyping(false);
  };

  // Format timestamp
  const formatTime = (timestamp) => {
    return new Date(timestamp).toLocaleTimeString([], { 
      hour: '2-digit', 
      minute: '2-digit' 
    });
  };

  const chatVariants = {
    hidden: { opacity: 0, scale: 0.8, y: 50 },
    visible: { 
      opacity: 1, 
      scale: 1, 
      y: 0,
      transition: {
        type: "spring",
        stiffness: 300,
        damping: 30
      }
    },
    exit: { 
      opacity: 0, 
      scale: 0.8, 
      y: 50,
      transition: {
        duration: 0.2
      }
    }
  };

  const messageVariants = {
    hidden: { opacity: 0, y: 20 },
    visible: { 
      opacity: 1, 
      y: 0,
      transition: {
        duration: 0.3,
        ease: "easeOut"
      }
    }
  };

  return (
    <>
      {/* Floating Chat Button */}
      <motion.button
        onClick={() => setIsOpen(!isOpen)}
        className="fixed bottom-6 right-6 w-16 h-16 bg-light-green hover:bg-primary-600 text-dark-bg rounded-full shadow-lg hover:shadow-xl transition-all duration-300 z-50 flex items-center justify-center group"
        title="BVMT Expert Financial Assistant"
        whileHover={{ scale: 1.1, rotate: 5 }}
        whileTap={{ scale: 0.9 }}
        animate={{ 
          boxShadow: isOpen ? "0 0 20px rgba(144, 238, 144, 0.5)" : "0 4px 20px rgba(0, 0, 0, 0.3)"
        }}
      >
        <motion.div
          animate={{ rotate: isOpen ? 360 : 0 }}
          transition={{ duration: 0.5 }}
        >
          <HiChatBubbleLeftRight className="w-8 h-8 group-hover:scale-110 transition-transform" />
        </motion.div>
        <AnimatePresence>
          {!isOpen && (
            <motion.span 
              className="absolute -top-2 -right-2 w-6 h-6 bg-crystal-white text-dark-bg text-xs rounded-full flex items-center justify-center animate-pulse font-display font-semibold"
              initial={{ scale: 0 }}
              animate={{ scale: 1 }}
              exit={{ scale: 0 }}
              transition={{ duration: 0.2 }}
            >
              BVMT
            </motion.span>
          )}
        </AnimatePresence>
      </motion.button>

      {/* Chat Interface */}
      <AnimatePresence>
        {isOpen && (
          <motion.div 
            className="fixed bottom-24 right-6 w-96 h-[600px] bg-background-secondary rounded-2xl shadow-2xl border border-light-silver flex flex-col z-50 glass"
            variants={chatVariants}
            initial="hidden"
            animate="visible"
            exit="exit"
          >
            {/* Header */}
            <motion.div 
              className="bg-light-green text-dark-bg p-4 rounded-t-2xl"
              initial={{ y: -20, opacity: 0 }}
              animate={{ y: 0, opacity: 1 }}
              transition={{ delay: 0.1 }}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center space-x-3">
                  <motion.div 
                    className="w-10 h-10 bg-dark-bg bg-opacity-20 rounded-full flex items-center justify-center"
                    whileHover={{ scale: 1.1, rotate: 360 }}
                    transition={{ duration: 0.3 }}
                  >
                    <HiChatBubbleLeftRight className="w-6 h-6" />
                  </motion.div>
                  <div>
                    <h3 className="font-display font-semibold">BVMT Expert</h3>
                    <p className="text-dark-bg text-sm opacity-80 font-sans">Tunisian Stock Exchange</p>
                  </div>
                </div>
                <div className="flex items-center space-x-2">
                  <motion.button
                    onClick={clearChat}
                    className="p-2 hover:bg-dark-bg hover:bg-opacity-20 rounded-lg transition-colors"
                    title="Clear Chat"
                    whileHover={{ scale: 1.1, rotate: 180 }}
                    whileTap={{ scale: 0.9 }}
                    transition={{ duration: 0.2 }}
                  >
                    <HiArrowPath className="w-4 h-4" />
                  </motion.button>
                  <motion.button
                    onClick={() => setIsOpen(false)}
                    className="p-2 hover:bg-dark-bg hover:bg-opacity-20 rounded-lg transition-colors"
                    title="Close Chat"
                    whileHover={{ scale: 1.1 }}
                    whileTap={{ scale: 0.9 }}
                    transition={{ duration: 0.2 }}
                  >
                    <HiXMark className="w-4 h-4" />
                  </motion.button>
                </div>
              </div>
            </motion.div>

            {/* Messages Area */}
            <motion.div 
              className="flex-1 overflow-y-auto p-4 space-y-4 bg-background-primary custom-scrollbar"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.2 }}
            >
              <AnimatePresence>
                {messages.map((message) => (
                  <motion.div
                    key={message.id}
                    className={`flex ${message.type === 'user' ? 'justify-end' : 'justify-start'}`}
                    variants={messageVariants}
                    initial="hidden"
                    animate="visible"
                    exit="hidden"
                  >
                    <motion.div
                      className={`max-w-[80%] p-3 rounded-2xl ${
                        message.type === 'user'
                          ? 'bg-light-green text-dark-bg rounded-br-md'
                          : 'bg-background-tertiary text-pure-white rounded-bl-md border border-light-silver'
                      }`}
                      whileHover={{ scale: 1.02 }}
                      transition={{ duration: 0.2 }}
                    >
                      <div className="whitespace-pre-wrap text-sm font-sans">{message.content}</div>
                      <div
                        className={`text-xs mt-2 font-mono ${
                          message.type === 'user' ? 'text-dark-bg opacity-70' : 'text-crystal-white'
                        }`}
                      >
                        {formatTime(message.timestamp)}
                      </div>
                    </motion.div>
                  </motion.div>
                ))}
              </AnimatePresence>
              
              {/* Typing Indicator */}
              <AnimatePresence>
                {isTyping && (
                  <motion.div 
                    className="flex justify-start"
                    initial={{ opacity: 0, x: -20 }}
                    animate={{ opacity: 1, x: 0 }}
                    exit={{ opacity: 0, x: -20 }}
                  >
                    <div className="bg-background-tertiary text-pure-white p-3 rounded-2xl rounded-bl-md border border-light-silver">
                      <div className="flex items-center space-x-2">
                        <div className="flex space-x-1">
                          <motion.div 
                            className="w-2 h-2 bg-light-green rounded-full"
                            animate={{ scale: [1, 1.5, 1] }}
                            transition={{ duration: 0.6, repeat: Infinity }}
                          ></motion.div>
                          <motion.div 
                            className="w-2 h-2 bg-light-green rounded-full"
                            animate={{ scale: [1, 1.5, 1] }}
                            transition={{ duration: 0.6, repeat: Infinity, delay: 0.2 }}
                          ></motion.div>
                          <motion.div 
                            className="w-2 h-2 bg-light-green rounded-full"
                            animate={{ scale: [1, 1.5, 1] }}
                            transition={{ duration: 0.6, repeat: Infinity, delay: 0.4 }}
                          ></motion.div>
                        </div>
                        <span className="text-sm text-crystal-white font-sans">BVMT Expert is analyzing...</span>
                      </div>
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>
              
              <div ref={messagesEndRef} />
            </motion.div>

            {/* Suggestions */}
            <AnimatePresence>
              {suggestions.length > 0 && messages.length <= 1 && (
                <motion.div 
                  className="p-4 bg-background-secondary border-t border-light-silver"
                  initial={{ height: 0, opacity: 0 }}
                  animate={{ height: "auto", opacity: 1 }}
                  exit={{ height: 0, opacity: 0 }}
                  transition={{ duration: 0.3 }}
                >
                  <div className="flex items-center space-x-2 mb-3">
                    <motion.div
                      animate={{ rotate: [0, 10, -10, 0] }}
                      transition={{ duration: 1, repeat: Infinity }}
                    >
                      <HiLightBulb className="w-4 h-4 text-light-green" />
                    </motion.div>
                    <span className="text-sm font-display font-medium text-pure-white">Suggested Questions</span>
                  </div>
                  <div className="grid grid-cols-1 gap-2">
                    {suggestions.slice(0, 3).map((suggestion, index) => (
                      <motion.button
                        key={index}
                        onClick={() => handleSuggestionClick(suggestion)}
                        className="text-left p-2 text-xs text-crystal-white hover:bg-background-tertiary hover:text-light-green rounded-lg transition-all duration-300 truncate font-sans"
                        title={suggestion}
                        whileHover={{ scale: 1.02, x: 5 }}
                        whileTap={{ scale: 0.98 }}
                        initial={{ opacity: 0, x: -20 }}
                        animate={{ opacity: 1, x: 0 }}
                        transition={{ delay: index * 0.1 }}
                      >
                        {suggestion}
                      </motion.button>
                    ))}
                  </div>
                </motion.div>
              )}
            </AnimatePresence>

            {/* Input Area */}
            <motion.div 
              className="p-4 bg-background-secondary border-t border-light-silver rounded-b-2xl"
              initial={{ y: 20, opacity: 0 }}
              animate={{ y: 0, opacity: 1 }}
              transition={{ delay: 0.3 }}
            >
              <form onSubmit={handleSubmit} className="flex space-x-2">
                <motion.input
                  ref={inputRef}
                  type="text"
                  value={inputMessage}
                  onChange={(e) => setInputMessage(e.target.value)}
                  placeholder="Ask about BVMT, Tunisian companies, or market news..."
                  className="flex-1 px-3 py-2 border border-light-silver rounded-lg focus:outline-none focus:ring-2 focus:ring-light-green focus:border-transparent text-sm bg-background-tertiary text-pure-white placeholder-crystal-white font-sans"
                  disabled={isTyping}
                  whileFocus={{ scale: 1.02 }}
                  transition={{ duration: 0.2 }}
                />
                <motion.button
                  type="submit"
                  disabled={!inputMessage.trim() || isTyping}
                  className="px-4 py-2 bg-light-green hover:bg-primary-600 disabled:bg-crystal-white disabled:opacity-50 text-dark-bg rounded-lg transition-all duration-300 disabled:cursor-not-allowed flex items-center justify-center font-display font-medium"
                  whileHover={{ scale: 1.05, rotate: 5 }}
                  whileTap={{ scale: 0.95 }}
                >
                  <motion.div
                    animate={{ rotate: isTyping ? 360 : 0 }}
                    transition={{ duration: 0.5, repeat: isTyping ? Infinity : 0 }}
                  >
                    <HiPaperAirplane className="w-4 h-4" />
                  </motion.div>
                </motion.button>
              </form>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
};

export default AIChatBot;

