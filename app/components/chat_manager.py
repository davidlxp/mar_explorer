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

from services import logs
from services.ai_workflow.mar_orchestrator import handle_user_query

logger = logging.getLogger(__name__)

class ChatManager:
    """Manages chat interface and interactions"""
    
    @staticmethod
    def display_chat_history() -> None:
        """Display existing chat history"""
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]):
                # Create a container for the message content
                msg_container = st.container()
                
                # Display the message content
                msg_container.write(msg["content"])
                
                # If it's an assistant message with citations, show them in an expander
                if msg["role"] == "assistant" and msg.get("citations"):
                    with msg_container.expander("🔍 View Sources"):
                        for citation in msg["citations"]:
                            source_type = citation.get("source", "Unknown")
                            reference = citation.get("reference", "No reference provided")
                            st.markdown(f"**{source_type}**: {reference}")
                
                # Handle any visualizations (keeping existing functionality)
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
            # Initialize messages list if it doesn't exist
            if "messages" not in st.session_state:
                st.session_state.messages = []
            
            # Keep only last 10 messages
            if len(st.session_state.messages) >= 40:  # 20 pairs of messages
                st.session_state.messages = st.session_state.messages[-19:]  # Keep last 19 to add new one
            
            # Add user message to chat and display immediately
            user_msg = {"role": "user", "content": prompt}
            st.session_state.messages.append(user_msg)
            
            # Display user message
            with st.chat_message("user"):
                st.write(prompt)
            
            # Get AI response
            with st.spinner("Thinking..."):
                answer_packet = handle_user_query(
                    user_query=prompt, 
                    history=st.session_state.messages
                )
                
                # Create assistant response
                response_msg = {
                    "role": "assistant",
                    "content": answer_packet.text,
                }
                
                # Add citations if available
                if answer_packet.citations:
                    response_msg["citations"] = answer_packet.citations
                
                # Add to session state
                st.session_state.messages.append(response_msg)
                
                # Display assistant response immediately
                with st.chat_message("assistant"):
                    # Create a container for the response
                    response_container = st.container()
                    
                    # Display the response text
                    response_container.write(answer_packet.text)
                    
                    # Add citations tooltip if available
                    if answer_packet.citations:
                        with response_container.expander("🔍 View Sources"):
                            for citation in answer_packet.citations:
                                source_type = citation.get("source", "Unknown")
                                reference = citation.get("reference", "No reference provided")
                                st.markdown(f"**{source_type}**: {reference}")
                
                # Log the interaction
                logs.log_question(
                    question=prompt,
                    response=answer_packet.text,
                    confidence=answer_packet.confidence,
                    citations=answer_packet.citations
                )
                
        except Exception as e:
            st.error("❌ Error processing your question")
            logger.error(f"Error in chat: {str(e)}")
            with st.expander("See error details"):
                st.exception(e)

    @staticmethod
    def render_chat_interface() -> None:
        """Render the chat interface"""
        # Main chat interface
        st.markdown("### 💬 Ask MAR")
        
        # Display chat history
        ChatManager.display_chat_history()
        
        # Chat input
        if prompt := st.chat_input("What the total trade volume looks like in August 2025?"):
            ChatManager.handle_user_input(prompt)
        
        # Clear chat button
        if st.session_state.messages and st.button("Clear Chat"):
            st.session_state.messages = []
            st.rerun()
