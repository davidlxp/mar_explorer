"""Chat interface management component"""
import json
import logging
import plotly.io as pio
import streamlit as st
import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from services import nlq, logs

logger = logging.getLogger(__name__)

class ChatManager:
    """Manages chat interface and interactions"""
    
    @staticmethod
    def display_chat_history() -> None:
        """Display existing chat history"""
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                st.write(msg["content"])
                if msg.get("visualization"):
                    try:
                        fig = pio.from_json(json.dumps(msg["visualization"]))
                        st.plotly_chart(fig, use_container_width=True)
                    except Exception as e:
                        logger.error(f"Error displaying visualization: {str(e)}")

    @staticmethod
    def handle_user_input(prompt: str) -> None:
        """Process user input and generate response"""
        try:
            # Add user message to chat and display immediately
            user_msg = {"role": "user", "content": prompt}
            st.session_state.messages.append(user_msg)
            
            # Display user message
            with st.chat_message("user"):
                st.write(prompt)
            
            # Get AI response
            with st.spinner("Thinking..."):
                answer, data, confidence = nlq.process_question(prompt)
                
                # Create assistant response
                response_msg = {
                    "role": "assistant",
                    "content": answer,
                }
                
                # Add visualization if available
                if data and data.get("visualization"):
                    response_msg["visualization"] = data["visualization"]
                
                # Add to session state
                st.session_state.messages.append(response_msg)
                
                # Display assistant response immediately
                with st.chat_message("assistant"):
                    st.write(answer)
                    if data and data.get("visualization"):
                        fig = pio.from_json(json.dumps(data["visualization"]))
                        st.plotly_chart(fig, use_container_width=True)
                
                # Log the interaction
                logs.log_question(
                    question=prompt,
                    response=answer,
                    confidence=confidence,
                    citations=data.get("results", []) if data else []
                )
                
        except Exception as e:
            st.error("❌ Error processing your question")
            logger.error(f"Error in chat: {str(e)}")
            with st.expander("See error details"):
                st.exception(e)

    @staticmethod
    def render_chat_interface() -> None:
        """Render the chat interface"""
        st.markdown("### 💬 Ask MAR")
        
        # Display chat history
        ChatManager.display_chat_history()
        
        # Chat input
        if prompt := st.chat_input("Ask about MAR data or press releases..."):
            ChatManager.handle_user_input(prompt)
        
        # Clear chat button
        if st.session_state.messages and st.button("Clear Chat"):
            st.session_state.messages = []
            st.rerun()
