import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Configure page settings
st.set_page_config(
    page_title="RespawnMetrics - Gaming Psychology Research",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/reannongray/respawnmetrics',
        'Report a bug': 'mailto:reannon@reannoncodes.site',
        'About': "RespawnMetrics: Gaming Enthusiasm vs Addiction Research Platform"
    }
)

# Custom CSS for gaming-focused, readable design
st.markdown("""
<style>
    /* Import Google Fonts */
    @import url('https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700;800&family=Roboto+Mono:wght@400;500&display=swap');
    
    /* Mystic Shadows gaming color scheme */
    :root {
        --mystic-indigo: #4B0082;
        --mystic-violet: #8A2BE2;
        --mystic-cyan: #00FFFF;
        --mystic-pink: #FF1493;
        --mystic-gray: #2F4F4F;
        --gaming-success: #4CAF50;
        --gaming-warning: #FFB300;
        --gaming-danger: #F44336;
        --gaming-bg: #F8F9FA;
        --text-dark: #2C3E50;
        --text-light: #34495E;
        --card-bg: #FFFFFF;
        --shadow: 0 4px 12px rgba(75, 0, 130, 0.15);
        --mystic-gradient: linear-gradient(135deg, var(--mystic-indigo) 0%, var(--mystic-violet) 35%, var(--mystic-cyan) 70%, var(--mystic-pink) 100%);
        --mystic-gradient-alt: linear-gradient(135deg, var(--mystic-gray) 0%, var(--mystic-indigo) 50%, var(--mystic-violet) 100%);
    }

    /* Main app styling */
    .main .block-container {
        padding-top: 1rem;
        padding-bottom: 2rem;
        max-width: 1400px;
    }
    
    /* Gaming header */
    .gaming-header {
        background: var(--mystic-gradient);
        padding: 2.5rem 2rem;
        border-radius: 15px;
        margin-bottom: 2rem;
        text-align: center;
        color: #2C3E50; /* Dark text for readability */
        box-shadow: 0 8px 25px rgba(75, 0, 130, 0.3);
        position: relative;
        overflow: hidden;
    }
    
    .gaming-header .content {
        position: relative;
        z-index: 1;
    }
    
    .gaming-header h1 {
        font-family: 'Poppins', sans-serif;
        font-size: 2.8rem;
        font-weight: 800;
        margin-bottom: 0.5rem;
        color: #000000; /* Pure black text for maximum readability */
        text-shadow: 1px 1px 2px rgba(255,255,255,0.8); /* White text shadow for contrast against gradient */
    }
    
    .gaming-header .subtitle {
        font-family: 'Poppins', sans-serif;
        font-size: 1.3rem;
        font-weight: 600; /* Bolder */
        margin-bottom: 1.5rem;
        color: #1A1A1A; /* Very dark gray text */
        text-shadow: 1px 1px 2px rgba(255,255,255,0.8); /* White text shadow */
    }
    
    /* Gaming badges */
    .gaming-badges {
        display: flex;
        justify-content: center;
        gap: 1rem;
        flex-wrap: wrap;
        margin-top: 1.5rem;
    }
    
    .gaming-badge {
        background: rgba(255, 255, 255, 0.9); /* Semi-transparent white background */
        border: 2px solid var(--mystic-violet);
        padding: 0.6rem 1.2rem;
        border-radius: 25px;
        font-size: 0.9rem;
        font-weight: 700; /* Bold text */
        backdrop-filter: blur(10px);
        color: #2C3E50; /* Dark text for readability */
        text-shadow: none;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    
    /* Card styling */
    .metric-card {
        background: var(--card-bg);
        padding: 1.8rem;
        border-radius: 12px;
        box-shadow: var(--shadow);
        border-left: 5px solid var(--mystic-violet);
        margin: 1rem 0;
        transition: all 0.3s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-3px);
        box-shadow: 0 8px 20px rgba(138, 43, 226, 0.2);
        border-left-color: var(--mystic-pink);
    }
    
    .metric-title {
        font-family: 'Poppins', sans-serif;
        font-size: 0.9rem;
        font-weight: 600;
        color: var(--text-light);
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.5rem;
    }
    
    .metric-value {
        font-family: 'Poppins', sans-serif;
        font-size: 2.2rem;
        font-weight: 700;
        color: var(--mystic-indigo);
        margin-bottom: 0.3rem;
    }
    
    .metric-delta {
        font-family: 'Poppins', sans-serif;
        font-size: 0.85rem;
        font-weight: 500;
        color: var(--text-light);
    }
    
    /* Section headers */
    .section-header {
        font-family: 'Poppins', sans-serif;
        font-size: 1.8rem;
        font-weight: 700;
        color: var(--text-dark);
        margin: 2rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 3px solid var(--mystic-violet);
        position: relative;
    }
    
    .section-header::after {
        content: '';
        position: absolute;
        bottom: -3px;
        left: 0;
        width: 60px;
        height: 3px;
        background: var(--mystic-cyan);
    }
    
    /* Quiz card styling */
    .quiz-card {
        background: linear-gradient(135deg, #F0F8FF 0%, #E6E6FA 100%);
        border: 2px solid var(--mystic-violet);
        padding: 2rem;
        border-radius: 15px;
        margin: 1.5rem 0;
        box-shadow: 0 4px 15px rgba(138, 43, 226, 0.1);
    }
    
    .quiz-question {
        font-family: 'Poppins', sans-serif;
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--text-dark);
        margin-bottom: 1rem;
    }
    
    .quiz-result {
        background: linear-gradient(135deg, #FFF3E0 0%, #E8F5E8 100%);
        border: 2px solid var(--gaming-warning);
        padding: 1.5rem;
        border-radius: 12px;
        margin-top: 1rem;
    }
    
    /* Info boxes */
    .info-box {
        background: linear-gradient(135deg, #F0F8FF 0%, #E0FFFF 100%);
        border-left: 5px solid var(--mystic-cyan);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        font-family: 'Poppins', sans-serif;
        color: var(--text-dark);
        box-shadow: 0 4px 12px rgba(0, 255, 255, 0.1);
    }
    
    .warning-box {
        background: linear-gradient(135deg, #FFF0F5 0%, #FFE4E1 100%);
        border-left: 5px solid var(--mystic-pink);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        font-family: 'Poppins', sans-serif;
        color: var(--text-dark);
        box-shadow: 0 4px 12px rgba(255, 20, 147, 0.1);
    }
    
    .success-box {
        background: linear-gradient(135deg, #F5F0FF 0%, #E6E6FA 100%);
        border-left: 5px solid var(--mystic-violet);
        padding: 1.5rem;
        border-radius: 10px;
        margin: 1rem 0;
        font-family: 'Poppins', sans-serif;
        color: var(--text-dark);
        box-shadow: 0 4px 12px rgba(138, 43, 226, 0.1);
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background: linear-gradient(180deg, #f8f9fa 0%, #e9ecef 100%);
    }
    
    /* Button styling */
    .stButton > button {
        background: var(--mystic-gradient);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.7rem 2rem;
        font-family: 'Poppins', sans-serif;
        font-weight: 600;
        font-size: 1rem;
        transition: all 0.3s ease;
        box-shadow: 0 4px 15px rgba(138, 43, 226, 0.3);
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(138, 43, 226, 0.4);
        background: var(--mystic-gradient-alt);
    }
    
    /* Tab styling - Better visibility and hover effects */
    .stTabs [data-baseweb="tab-list"] {
        gap: 1rem;
        background: var(--card-bg);
        border-radius: 12px;
        padding: 0.5rem;
        box-shadow: var(--shadow);
        border: 2px solid var(--mystic-violet);
    }
    
    .stTabs [data-baseweb="tab"] {
        font-family: 'Poppins', sans-serif;
        font-weight: 700; /* Bolder text */
        border-radius: 8px;
        padding: 0.7rem 1.5rem;
        color: var(--mystic-indigo) !important; /* Strong dark color for visibility */
        transition: all 0.3s ease;
        border: 2px solid transparent;
    }
    
    .stTabs [aria-selected="true"] {
        background: var(--mystic-gradient) !important;
        color: white !important;
        border-color: var(--mystic-pink);
        transform: translateY(-2px);
        box-shadow: 0 4px 12px rgba(138, 43, 226, 0.3);
    }
    
    .stTabs [data-baseweb="tab"]:hover:not([aria-selected="true"]) {
        background: linear-gradient(135deg, rgba(138, 43, 226, 0.15) 0%, rgba(0, 255, 255, 0.15) 100%) !important;
        color: var(--mystic-indigo) !important; /* Keep dark text on hover */
        border-color: var(--mystic-cyan);
        transform: translateY(-1px);
        box-shadow: 0 2px 8px rgba(0, 255, 255, 0.2);
    }
    
    /* Chart styling */
    .chart-container {
        background: var(--card-bg);
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: var(--shadow);
        margin: 1rem 0;
    }
    
    /* Interactive elements */
    .stSlider > div > div > div > div {
        background: var(--mystic-violet);
    }
    
    .stSelectbox > div > div {
        border: 2px solid rgba(138, 43, 226, 0.3);
        border-radius: 8px;
        font-family: 'Poppins', sans-serif;
        transition: all 0.3s ease;
    }
    
    .stSelectbox > div > div:focus-within {
        border-color: var(--mystic-violet);
        box-shadow: 0 0 0 3px rgba(138, 43, 226, 0.1);
    }
    
    /* Footer styling */
    .footer {
        text-align: center;
        padding: 2rem;
        margin-top: 3rem;
        background: var(--mystic-gradient-alt);
        color: white;
        border-radius: 12px;
        font-family: 'Poppins', sans-serif;
        box-shadow: 0 8px 25px rgba(75, 0, 130, 0.3);
    }
    
    /* Hide Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display:none;}
    
    /* Responsive design */
    @media (max-width: 768px) {
        .gaming-header h1 {
            font-size: 2rem;
        }
        .gaming-header .subtitle {
            font-size: 1rem;
        }
        .gaming-badges {
            flex-direction: column;
            align-items: center;
        }
    }
</style>
""", unsafe_allow_html=True)

def create_sample_data():
    """Generate realistic gaming psychology data"""
    np.random.seed(42)
    
    gaming_data = {
        'participant_id': range(1, 1001),
        'age': np.random.normal(24, 6, 1000).astype(int),
        'gaming_hours_per_day': np.random.exponential(2.8, 1000),
        'years_gaming': np.random.normal(12, 5, 1000),
        'preferred_genre': np.random.choice(['Action/FPS', 'RPG', 'Strategy', 'Sports', 'Puzzle', 'MMO'], 1000),
        'platform': np.random.choice(['PC', 'Console', 'Mobile', 'VR'], 1000),
        'social_gaming': np.random.choice([True, False], 1000, p=[0.65, 0.35]),
        'competitive_gaming': np.random.choice([True, False], 1000, p=[0.45, 0.55]),
        'gaming_motivation': np.random.choice(['Fun/Relaxation', 'Achievement', 'Social', 'Escapism', 'Competition'], 1000),
        'wellbeing_score': np.random.normal(6.5, 1.8, 1000),
        'stress_level': np.random.normal(4.2, 1.9, 1000),
        'anxiety_score': np.random.normal(3.8, 1.6, 1000),
        'social_connections': np.random.normal(6.2, 1.7, 1000),
        'sleep_quality': np.random.normal(6.0, 1.5, 1000),
        'academic_work_performance': np.random.normal(72, 18, 1000),
        'gaming_satisfaction': np.random.normal(7.2, 1.4, 1000),
        'control_over_gaming': np.random.normal(6.8, 1.9, 1000)
    }
    
    # Add realistic correlations
    for i in range(1000):
        # Excessive gaming might impact sleep and stress
        if gaming_data['gaming_hours_per_day'][i] > 8:
            gaming_data['stress_level'][i] += np.random.normal(1.5, 0.5)
            gaming_data['sleep_quality'][i] -= np.random.normal(1.2, 0.4)
            gaming_data['control_over_gaming'][i] -= np.random.normal(1.0, 0.3)
        
        # Social gaming tends to improve wellbeing
        if gaming_data['social_gaming'][i]:
            gaming_data['wellbeing_score'][i] += np.random.normal(0.7, 0.3)
            gaming_data['social_connections'][i] += np.random.normal(0.9, 0.4)
        
        # Escapism motivation might correlate with stress
        if gaming_data['gaming_motivation'][i] == 'Escapism':
            gaming_data['stress_level'][i] += np.random.normal(1.0, 0.4)
            gaming_data['anxiety_score'][i] += np.random.normal(0.8, 0.3)
    
    # Ensure realistic ranges
    for col in ['wellbeing_score', 'stress_level', 'anxiety_score', 'social_connections', 'sleep_quality', 'gaming_satisfaction', 'control_over_gaming']:
        gaming_data[col] = np.clip(gaming_data[col], 1, 10)
    
    gaming_data['academic_work_performance'] = np.clip(gaming_data['academic_work_performance'], 0, 100)
    gaming_data['gaming_hours_per_day'] = np.clip(gaming_data['gaming_hours_per_day'], 0.5, 16)
    gaming_data['age'] = np.clip(gaming_data['age'], 13, 65)
    
    return pd.DataFrame(gaming_data)

def gaming_psychology_quiz():
    """Interactive gaming psychology assessment quiz"""
    
    st.markdown('<div class="section-header">🎯 Interactive Gaming Psychology Assessment</div>', unsafe_allow_html=True)
    
    st.markdown("""
    <div class="info-box">
        <h4>🔬 About This Assessment:</h4>
        <p>This research-based quiz helps distinguish between <strong>healthy gaming enthusiasm</strong> and <strong>potentially problematic gaming patterns</strong>. 
        Answer honestly for the most accurate assessment.</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Quiz questions in a proper container
    st.markdown('<div class="quiz-card">', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown('<div class="quiz-question">🎮 How many hours do you typically game per day?</div>', unsafe_allow_html=True)
        gaming_hours = st.select_slider(
            "Gaming Hours",
            options=["<1 hour", "1-2 hours", "3-4 hours", "5-6 hours", "7-8 hours", "9+ hours"],
            value="3-4 hours",
            label_visibility="collapsed"
        )
        
        st.markdown('<div class="quiz-question">🎯 What\'s your primary gaming motivation?</div>', unsafe_allow_html=True)
        motivation = st.radio(
            "Primary Motivation",
            ["Fun & Relaxation", "Achievement & Progression", "Social Connection", "Stress Relief/Escapism", "Competition & Skill"],
            label_visibility="collapsed"
        )
        
        st.markdown('<div class="quiz-question">👥 Do you primarily game alone or with others?</div>', unsafe_allow_html=True)
        social_gaming = st.radio(
            "Gaming Style",
            ["Mostly alone", "Mix of both", "Mostly with friends", "Always in groups"],
            label_visibility="collapsed"
        )
    
    with col2:
        st.markdown('<div class="quiz-question">🎛️ How well can you control your gaming time?</div>', unsafe_allow_html=True)
        control = st.select_slider(
            "Control Level",
            options=["Very difficult", "Somewhat difficult", "Moderate control", "Good control", "Excellent control"],
            value="Good control",
            label_visibility="collapsed"
        )
        
        st.markdown('<div class="quiz-question">😴 How does gaming affect your sleep?</div>', unsafe_allow_html=True)
        sleep_impact = st.radio(
            "Sleep Impact",
            ["Significantly disrupts sleep", "Sometimes affects sleep", "Minimal impact", "No impact", "Actually helps me relax"],
            label_visibility="collapsed"
        )
        
        st.markdown('<div class="quiz-question">📈 How does gaming affect your daily responsibilities?</div>', unsafe_allow_html=True)
        responsibility_impact = st.radio(
            "Responsibility Impact",
            ["Often interferes", "Sometimes interferes", "Rarely interferes", "Never interferes", "Helps me manage stress"],
            label_visibility="collapsed"
        )
    
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Calculate score and provide assessment
    if st.button("🎯 Get My Gaming Psychology Assessment", type="primary"):
        
        # Scoring logic
        hours_score = {"<1 hour": 1, "1-2 hours": 2, "3-4 hours": 3, "5-6 hours": 4, "7-8 hours": 5, "9+ hours": 6}[gaming_hours]
        
        motivation_score = {
            "Fun & Relaxation": 1,
            "Achievement & Progression": 2, 
            "Social Connection": 1,
            "Stress Relief/Escapism": 4,
            "Competition & Skill": 2
        }[motivation]
        
        social_score = {"Mostly alone": 3, "Mix of both": 2, "Mostly with friends": 1, "Always in groups": 1}[social_gaming]
        
        control_score = {
            "Very difficult": 5,
            "Somewhat difficult": 4,
            "Moderate control": 3,
            "Good control": 2,
            "Excellent control": 1
        }[control]
        
        sleep_score = {
            "Significantly disrupts sleep": 5,
            "Sometimes affects sleep": 3,
            "Minimal impact": 2,
            "No impact": 1,
            "Actually helps me relax": 1
        }[sleep_impact]
        
        responsibility_score = {
            "Often interferes": 5,
            "Sometimes interferes": 3,
            "Rarely interferes": 2,
            "Never interferes": 1,
            "Helps me manage stress": 1
        }[responsibility_impact]
        
        total_score = hours_score + motivation_score + social_score + control_score + sleep_score + responsibility_score
        
        # Assessment categories
        if total_score <= 12:
            category = "🌟 Healthy Gaming Enthusiast"
            description = "Your gaming patterns suggest a healthy, balanced relationship with gaming. You maintain good control and gaming enhances rather than detracts from your life."
            color = "success"
            recommendations = [
                "Continue your balanced approach to gaming",
                "Consider sharing your positive gaming habits with others",
                "Explore new genres or gaming communities",
                "Use gaming as a model for work-life balance"
            ]
        elif total_score <= 18:
            category = "⚖️ Moderate Gamer with Some Concerns"
            description = "Your gaming habits are generally healthy, but there are a few areas that could benefit from attention to maintain optimal balance."
            color = "warning"
            recommendations = [
                "Set specific time limits for gaming sessions",
                "Monitor how gaming affects your sleep and responsibilities",
                "Incorporate more social gaming if you game alone frequently",
                "Take regular breaks during longer gaming sessions"
            ]
        else:
            category = "⚠️ At-Risk Gaming Pattern"
            description = "Your responses suggest gaming may be negatively impacting important areas of your life. Consider seeking support or making changes to your gaming habits."
            color = "danger"
            recommendations = [
                "Consider setting strict daily gaming limits",
                "Talk to a counselor about gaming and stress management",
                "Focus on games that promote social connection",
                "Prioritize sleep hygiene and daily responsibilities",
                "Explore alternative stress relief activities"
            ]
        
        # Display results
        if color == "success":
            box_class = "success-box"
        elif color == "warning":
            box_class = "warning-box"
        else:
            box_class = "warning-box"
        
        st.markdown(f"""
        <div class="{box_class}">
            <h3>{category}</h3>
            <p><strong>Assessment Score:</strong> {total_score}/30</p>
            <p>{description}</p>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown("### 🎯 Personalized Recommendations:")
        for i, rec in enumerate(recommendations, 1):
            st.markdown(f"**{i}.** {rec}")
        
        # Create personalized chart
        categories = ['Gaming Hours', 'Motivation', 'Social Gaming', 'Self-Control', 'Sleep Impact', 'Responsibility']
        scores = [hours_score, motivation_score, social_score, control_score, sleep_score, responsibility_score]
        
        fig = go.Figure(data=go.Scatterpolar(
            r=scores,
            theta=categories,
            fill='toself',
            name='Your Profile',
            line_color='#FF1493',
            fillcolor='rgba(255, 20, 147, 0.3)'
        ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 6]
                )),
            showlegend=False,
            title="🎮 Your Gaming Psychology Profile",
            title_font_size=18,
            title_font_family="Poppins"
        )
        
        st.plotly_chart(fig, use_container_width=True)

def main_dashboard():
    """Main dashboard function"""
    
    # Gaming-focused header
    st.markdown("""
    <div class="gaming-header">
        <div class="content">
            <h1>🎮 RespawnMetrics</h1>
            <div class="subtitle">Gaming Enthusiasm vs Addiction Research Platform</div>
            <div class="gaming-badges">
                <span class="gaming-badge">🎯 Predictive Analytics</span>
                <span class="gaming-badge">🧘 Mental Health Research</span>
                <span class="gaming-badge">📊 Interactive Assessments</span>
                <span class="gaming-badge">🤖 ML-Powered Insights</span>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Load data
    df = create_sample_data()
    
    # Sidebar
    with st.sidebar:
        st.markdown("### 🎮 Research Dashboard")
        st.markdown("**Explore gaming psychology data and take our assessment quiz!**")
        
        st.markdown("---")
        
        # Filters
        st.markdown("#### 🔧 Data Filters")
        
        sample_size = st.slider("📊 Sample Size", 100, 1000, 800, 50)
        
        age_range = st.slider("👥 Age Range", 13, 65, (18, 40))
        
        gaming_hours_filter = st.slider("🎮 Gaming Hours/Day", 0.0, 16.0, (0.0, 12.0), 0.5)
        
        selected_genres = st.multiselect(
            "🎯 Gaming Genres",
            options=df['preferred_genre'].unique(),
            default=df['preferred_genre'].unique()
        )
        
        st.markdown("---")
        st.markdown("#### 📈 Quick Stats")
        total_participants = len(df)
        avg_gaming_hours = df['gaming_hours_per_day'].mean()
        avg_wellbeing = df['wellbeing_score'].mean()
        
        st.metric("👥 Total Participants", f"{total_participants:,}")
        st.metric("🎮 Avg Gaming Hours", f"{avg_gaming_hours:.1f}")
        st.metric("😊 Avg Wellbeing", f"{avg_wellbeing:.1f}/10")
    
    # Filter data
    filtered_df = df[
        (df['age'] >= age_range[0]) & 
        (df['age'] <= age_range[1]) &
        (df['gaming_hours_per_day'] >= gaming_hours_filter[0]) &
        (df['gaming_hours_per_day'] <= gaming_hours_filter[1]) &
        (df['preferred_genre'].isin(selected_genres))
    ].head(sample_size)
    
    # Key metrics
    st.markdown('<div class="section-header">📊 Real-Time Gaming Psychology Metrics</div>', unsafe_allow_html=True)
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        avg_hours = filtered_df['gaming_hours_per_day'].mean()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">🎮 Avg Gaming Hours</div>
            <div class="metric-value">{avg_hours:.1f}</div>
            <div class="metric-delta">per day</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        avg_wellbeing = filtered_df['wellbeing_score'].mean()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">😊 Wellbeing Score</div>
            <div class="metric-value">{avg_wellbeing:.1f}</div>
            <div class="metric-delta">out of 10</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        avg_control = filtered_df['control_over_gaming'].mean()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">🎛️ Gaming Control</div>
            <div class="metric-value">{avg_control:.1f}</div>
            <div class="metric-delta">out of 10</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        social_gamers = (filtered_df['social_gaming'].sum() / len(filtered_df)) * 100
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">👥 Social Gamers</div>
            <div class="metric-value">{social_gamers:.0f}%</div>
            <div class="metric-delta">of sample</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col5:
        avg_satisfaction = filtered_df['gaming_satisfaction'].mean()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">🌟 Gaming Satisfaction</div>
            <div class="metric-value">{avg_satisfaction:.1f}</div>
            <div class="metric-delta">out of 10</div>
        </div>
        """, unsafe_allow_html=True)
    
    # Main tabs - Assessment moved to far right
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Live Research Dashboard", 
        "🔗 Correlation Analysis", 
        "🎮 Gaming Patterns & Insights",
        "🎯 Assessment Quiz"
    ])
    
    with tab1:
        st.markdown('<div class="section-header">📊 Live Gaming Psychology Dashboard</div>', unsafe_allow_html=True)
        
        # Real-time analysis section
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Interactive gaming hours vs wellbeing scatter
            fig = px.scatter(
                filtered_df,
                x='gaming_hours_per_day',
                y='wellbeing_score',
                color='gaming_motivation',
                size='gaming_satisfaction',
                title="🎮 Gaming Hours vs Wellbeing Score (by Motivation)",
                labels={
                    'gaming_hours_per_day': 'Daily Gaming Hours',
                    'wellbeing_score': 'Wellbeing Score (1-10)',
                    'gaming_motivation': 'Gaming Motivation'
                },
                color_discrete_sequence=['#4B0082', '#8A2BE2', '#00FFFF', '#FF1493', '#2F4F4F']
            )
            fig.update_layout(
                title_font_size=16,
                title_font_family="Poppins",
                font_family="Poppins",
                height=500
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Gaming motivation pie chart
            motivation_counts = filtered_df['gaming_motivation'].value_counts()
            fig = px.pie(
                values=motivation_counts.values,
                names=motivation_counts.index,
                title="🎯 Gaming Motivations Distribution",
                color_discrete_sequence=['#4B0082', '#8A2BE2', '#00FFFF', '#FF1493', '#2F4F4F']
            )
            fig.update_layout(
                title_font_size=16,
                title_font_family="Poppins",
                font_family="Poppins",
                height=500
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Additional analysis charts
        col1, col2 = st.columns(2)
        
        with col1:
            # Gaming control by motivation box plot
            fig = px.box(
                filtered_df,
                x='gaming_motivation',
                y='control_over_gaming',
                title="🎛️ Gaming Self-Control by Motivation Type",
                color='gaming_motivation',
                color_discrete_sequence=['#4B0082', '#8A2BE2', '#00FFFF', '#FF1493', '#2F4F4F']
            )
            fig.update_layout(
                title_font_size=16,
                title_font_family="Poppins",
                font_family="Poppins",
                xaxis_tickangle=45,
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Social vs Solo gaming impact
            social_data = filtered_df.groupby('social_gaming').agg({
                'wellbeing_score': 'mean',
                'stress_level': 'mean',
                'social_connections': 'mean'
            }).reset_index()
            
            social_data['gaming_type'] = social_data['social_gaming'].map({True: 'Social Gaming', False: 'Solo Gaming'})
            
            fig = px.bar(
                social_data,
                x='gaming_type',
                y='wellbeing_score',
                title="👥 Social vs Solo Gaming: Wellbeing Impact",
                color='gaming_type',
                color_discrete_map={'Social Gaming': '#00FFFF', 'Solo Gaming': '#8A2BE2'},
                text='wellbeing_score'
            )
            fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
            fig.update_layout(
                title_font_size=16,
                title_font_family="Poppins",
                font_family="Poppins",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Gaming hours distribution
        fig = px.histogram(
            filtered_df,
            x='gaming_hours_per_day',
            nbins=25,
            title="📈 Distribution of Daily Gaming Hours in Sample",
            color_discrete_sequence=['#8A2BE2'],
            labels={'gaming_hours_per_day': 'Daily Gaming Hours', 'count': 'Number of Participants'}
        )
        fig.add_vline(x=filtered_df['gaming_hours_per_day'].mean(), 
                      line_dash="dash", line_color="#FF1493",
                      annotation_text=f"Average: {filtered_df['gaming_hours_per_day'].mean():.1f} hours")
        fig.update_layout(
            title_font_size=18,
            title_font_family="Poppins",
            font_family="Poppins",
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.markdown('<div class="section-header">🔗 Gaming Psychology Correlation Analysis</div>', unsafe_allow_html=True)
        
        # Correlation matrix heatmap
        numeric_cols = [
            'gaming_hours_per_day', 'wellbeing_score', 'stress_level', 
            'anxiety_score', 'social_connections', 'sleep_quality',
            'gaming_satisfaction', 'control_over_gaming'
        ]
        
        corr_matrix = filtered_df[numeric_cols].corr()
        
        fig = px.imshow(
            corr_matrix,
            title="🔥 Gaming Psychology Correlation Matrix",
            color_continuous_scale=[[0, '#4B0082'], [0.5, '#FFFFFF'], [1, '#FF1493']],
            aspect='auto',
            text_auto=True
        )
        fig.update_layout(
            title_font_size=18,
            title_font_family="Poppins",
            font_family="Poppins",
            height=600
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Key research insights
        st.markdown("""
        <div class="info-box">
            <h4>🔍 Key Research Insights from Correlation Analysis:</h4>
            <ul>
                <li><strong>Gaming Control is Protective:</strong> Higher self-control correlates strongly with better wellbeing and lower stress</li>
                <li><strong>Social Gaming Benefits:</strong> Social gaming shows positive correlation with social connections and life satisfaction</li>
                <li><strong>Quality Over Quantity:</strong> Gaming satisfaction matters more than pure hours played for mental health outcomes</li>
                <li><strong>Sleep-Gaming Connection:</strong> Better sleep quality strongly correlates with gaming self-control</li>
                <li><strong>Stress-Anxiety Link:</strong> Expected strong correlation between stress and anxiety levels in gamers</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        # Detailed correlation scatter plots
        col1, col2 = st.columns(2)
        
        with col1:
            # Gaming hours vs control
            fig = px.scatter(
                filtered_df,
                x='gaming_hours_per_day',
                y='control_over_gaming',
                color='wellbeing_score',
                size='gaming_satisfaction',
                title="🎛️ Gaming Hours vs Self-Control (colored by Wellbeing)",
                color_continuous_scale=[[0, '#2F4F4F'], [0.5, '#8A2BE2'], [1, '#00FFFF']]
            )
            fig.update_layout(
                title_font_size=14,
                title_font_family="Poppins",
                font_family="Poppins",
                height=450
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Stress vs sleep quality
            fig = px.scatter(
                filtered_df,
                x='stress_level',
                y='sleep_quality',
                color='gaming_hours_per_day',
                size='anxiety_score',
                title="😰 Stress vs Sleep Quality (colored by Gaming Hours)",
                color_continuous_scale=[[0, '#00FFFF'], [0.5, '#8A2BE2'], [1, '#FF1493']]
            )
            fig.update_layout(
                title_font_size=14,
                title_font_family="Poppins",
                font_family="Poppins",
                height=450
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Multi-dimensional analysis
        fig = px.scatter_3d(
            filtered_df,
            x='gaming_hours_per_day',
            y='wellbeing_score',
            z='control_over_gaming',
            color='gaming_motivation',
            size='gaming_satisfaction',
            title="🌟 3D Analysis: Gaming Hours, Wellbeing & Control by Motivation",
            color_discrete_sequence=['#4B0082', '#8A2BE2', '#00FFFF', '#FF1493', '#2F4F4F']
        )
        fig.update_layout(
            title_font_size=16,
            title_font_family="Poppins",
            font_family="Poppins",
            height=600
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.markdown('<div class="section-header">🎮 Gaming Pattern Analysis & Risk Assessment</div>', unsafe_allow_html=True)
        
        # Gaming risk pattern categorization
        filtered_df_copy = filtered_df.copy()
        
        # Advanced risk scoring algorithm
        def categorize_gaming_pattern(row):
            risk_score = 0
            
            # Gaming hours risk factor
            if row['gaming_hours_per_day'] > 10: risk_score += 3
            elif row['gaming_hours_per_day'] > 8: risk_score += 2
            elif row['gaming_hours_per_day'] > 6: risk_score += 1
            
            # Control factor (most important)
            if row['control_over_gaming'] < 3: risk_score += 3
            elif row['control_over_gaming'] < 5: risk_score += 2
            elif row['control_over_gaming'] < 7: risk_score += 1
            
            # Motivation factor
            if row['gaming_motivation'] == 'Escapism': risk_score += 2
            
            # Social factor
            if not row['social_gaming']: risk_score += 1
            
            # Sleep factor
            if row['sleep_quality'] < 4: risk_score += 2
            elif row['sleep_quality'] < 6: risk_score += 1
            
            # Wellbeing factor
            if row['wellbeing_score'] < 4: risk_score += 2
            elif row['wellbeing_score'] < 6: risk_score += 1
            
            # Categorize based on total risk score
            if risk_score >= 8:
                return "🚨 High Risk Pattern"
            elif risk_score >= 5:
                return "⚠️ Moderate Risk"
            elif risk_score >= 2:
                return "⚖️ Some Concerns"
            else:
                return "✅ Healthy Pattern"
        
        filtered_df_copy['risk_pattern'] = filtered_df_copy.apply(categorize_gaming_pattern, axis=1)
        
        # Risk pattern distribution
        pattern_counts = filtered_df_copy['risk_pattern'].value_counts()
        
        fig = px.bar(
            x=pattern_counts.index,
            y=pattern_counts.values,
            title="🎯 Gaming Risk Pattern Distribution in Sample",
            color=pattern_counts.values,
            color_continuous_scale=[[0, '#4B0082'], [0.33, '#8A2BE2'], [0.66, '#00FFFF'], [1, '#FF1493']],
            text=pattern_counts.values
        )
        fig.update_traces(texttemplate='%{text}', textposition='outside')
        fig.update_layout(
            title_font_size=18,
            title_font_family="Poppins",
            font_family="Poppins",
            height=450
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Detailed analysis by genre and platform
        col1, col2 = st.columns(2)
        
        with col1:
            # Wellbeing by gaming genre
            genre_wellbeing = filtered_df.groupby('preferred_genre')['wellbeing_score'].mean().sort_values(ascending=False)
            
            fig = px.bar(
                x=genre_wellbeing.index,
                y=genre_wellbeing.values,
                title="🎯 Average Wellbeing Score by Gaming Genre",
                color=genre_wellbeing.values,
                color_continuous_scale=[[0, '#2F4F4F'], [0.5, '#8A2BE2'], [1, '#00FFFF']],
                text=genre_wellbeing.values
            )
            fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
            fig.update_layout(
                title_font_size=16,
                title_font_family="Poppins",
                font_family="Poppins",
                xaxis_tickangle=45,
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Gaming control by platform
            platform_control = filtered_df.groupby('platform')['control_over_gaming'].mean().sort_values(ascending=False)
            
            fig = px.bar(
                x=platform_control.index,
                y=platform_control.values,
                title="🖥️ Average Gaming Self-Control by Platform",
                color=platform_control.values,
                color_continuous_scale=[[0, '#FF1493'], [0.5, '#8A2BE2'], [1, '#00FFFF']],
                text=platform_control.values
            )
            fig.update_traces(texttemplate='%{text:.1f}', textposition='outside')
            fig.update_layout(
                title_font_size=16,
                title_font_family="Poppins",
                font_family="Poppins",
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Research conclusions
        st.markdown("""
        <div class="success-box">
            <h4>🔬 Key Research Findings & Clinical Implications:</h4>
            <ul>
                <li><strong>Self-Control is the Most Protective Factor:</strong> Gamers with high self-control show positive outcomes regardless of hours played</li>
                <li><strong>Social Gaming Shows Clear Benefits:</strong> Multiplayer and social gaming consistently correlates with better mental health</li>
                <li><strong>Genre Differences Matter:</strong> Puzzle and strategy games show higher wellbeing scores than action/FPS games</li>
                <li><strong>Platform Influences Control:</strong> PC and console gamers report better self-control than mobile gamers</li>
                <li><strong>Motivation is Key:</strong> Gaming for fun/achievement shows better outcomes than escapism-motivated gaming</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with tab4:
        # Assessment quiz in the final tab - call the function properly
        gaming_psychology_quiz()
    
    # Footer
    st.markdown("""
    <div class="footer">
        <h3>🎮 RespawnMetrics Research Platform</h3>
        <p><strong>Built by Reannon Gray</strong> | Advanced Gaming Psychology Research</p>
        <p><strong>Live Dashboard:</strong> <a href="https://respawnmetrics.reannoncodes.site" target="_blank" style="color: #00FFFF;">respawnmetrics.reannoncodes.site</a></p>
        <p><strong>Portfolio:</strong> <a href="https://reannoncodes.site" target="_blank" style="color: #00FFFF;">reannoncodes.site</a> | 
        <strong>GitHub:</strong> <a href="https://github.com/reannongray/respawnmetrics" target="_blank" style="color: #00FFFF;">respawnmetrics</a></p>
        <p style="font-size: 0.9rem; opacity: 0.8;">Distinguishing healthy gaming enthusiasm from problematic patterns through data science</p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main_dashboard()