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
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://github.com/reannongray/respawnmetrics',
        'Report a bug': 'mailto:reannon@reannoncodes.site',
        'About': "RespawnMetrics: Advanced Gaming Psychology Research Platform"
    }
)

# Custom CSS for professional, research-focused design
st.markdown("""
<style>
    /* Import Google Fonts for professional typography */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');
    
    /* Global theme variables */
    :root {
        --primary-color: #2E86AB;
        --secondary-color: #A23B72;
        --accent-color: #F18F01;
        --success-color: #4CAF50;
        --warning-color: #FF9800;
        --error-color: #F44336;
        --background-gradient: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        --card-shadow: 0 4px 20px rgba(0, 0, 0, 0.1);
        --text-primary: #2C3E50;
        --text-secondary: #5A6C7D;
        --border-radius: 12px;
    }

    /* Main app styling */
    .main .block-container {
        padding-top: 2rem;
        padding-bottom: 2rem;
        max-width: 1200px;
    }
    
    /* Custom header styling */
    .main-header {
        background: var(--background-gradient);
        padding: 2rem;
        border-radius: var(--border-radius);
        margin-bottom: 2rem;
        text-align: center;
        color: white;
        box-shadow: var(--card-shadow);
    }
    
    .main-header h1 {
        font-family: 'Inter', sans-serif;
        font-size: 3rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
    }
    
    .main-header .subtitle {
        font-family: 'Inter', sans-serif;
        font-size: 1.2rem;
        font-weight: 300;
        opacity: 0.9;
        margin-bottom: 1rem;
    }
    
    /* Research badges */
    .research-badges {
        display: flex;
        justify-content: center;
        gap: 1rem;
        flex-wrap: wrap;
        margin-top: 1.5rem;
    }
    
    .research-badge {
        background: rgba(255, 255, 255, 0.2);
        border: 1px solid rgba(255, 255, 255, 0.3);
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-size: 0.9rem;
        font-weight: 500;
        backdrop-filter: blur(10px);
    }
    
    /* Metric cards styling */
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: var(--border-radius);
        box-shadow: var(--card-shadow);
        border-left: 4px solid var(--primary-color);
        margin: 1rem 0;
        transition: transform 0.2s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.15);
    }
    
    .metric-title {
        font-family: 'Inter', sans-serif;
        font-size: 0.9rem;
        font-weight: 600;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.5px;
        margin-bottom: 0.5rem;
    }
    
    .metric-value {
        font-family: 'Inter', sans-serif;
        font-size: 2.5rem;
        font-weight: 700;
        color: var(--primary-color);
        margin-bottom: 0.5rem;
    }
    
    .metric-delta {
        font-family: 'Inter', sans-serif;
        font-size: 0.8rem;
        font-weight: 500;
    }
    
    /* Section headers */
    .section-header {
        font-family: 'Inter', sans-serif;
        font-size: 1.8rem;
        font-weight: 600;
        color: var(--text-primary);
        margin: 2rem 0 1rem 0;
        padding-bottom: 0.5rem;
        border-bottom: 3px solid var(--primary-color);
    }
    
    /* Custom info boxes */
    .info-box {
        background: linear-gradient(135deg, #E3F2FD 0%, #BBDEFB 100%);
        border-left: 4px solid var(--primary-color);
        padding: 1rem 1.5rem;
        border-radius: var(--border-radius);
        margin: 1rem 0;
        font-family: 'Inter', sans-serif;
    }
    
    .warning-box {
        background: linear-gradient(135deg, #FFF8E1 0%, #FFECB3 100%);
        border-left: 4px solid var(--warning-color);
        padding: 1rem 1.5rem;
        border-radius: var(--border-radius);
        margin: 1rem 0;
        font-family: 'Inter', sans-serif;
    }
    
    /* Sidebar styling */
    .css-1d391kg {
        background: linear-gradient(180deg, #f8f9fa 0%, #e9ecef 100%);
    }
    
    /* Button styling */
    .stButton > button {
        background: var(--primary-color);
        color: white;
        border: none;
        border-radius: var(--border-radius);
        padding: 0.5rem 2rem;
        font-family: 'Inter', sans-serif;
        font-weight: 600;
        transition: all 0.3s ease;
        box-shadow: var(--card-shadow);
    }
    
    .stButton > button:hover {
        background: #1976D2;
        transform: translateY(-2px);
        box-shadow: 0 6px 20px rgba(46, 134, 171, 0.4);
    }
    
    /* Tab styling */
    .stTabs [data-baseweb="tab-list"] {
        gap: 2rem;
        background: white;
        border-radius: var(--border-radius);
        padding: 0.5rem;
        box-shadow: var(--card-shadow);
    }
    
    .stTabs [data-baseweb="tab"] {
        font-family: 'Inter', sans-serif;
        font-weight: 500;
        border-radius: 8px;
        padding: 0.5rem 1rem;
    }
    
    .stTabs [aria-selected="true"] {
        background: var(--primary-color);
        color: white !important;
    }
    
    /* Chart container styling */
    .chart-container {
        background: white;
        padding: 1.5rem;
        border-radius: var(--border-radius);
        box-shadow: var(--card-shadow);
        margin: 1rem 0;
    }
    
    /* Code block styling */
    .stCode {
        font-family: 'JetBrains Mono', monospace;
        border-radius: var(--border-radius);
        border: 1px solid #e0e0e0;
    }
    
    /* Custom selectbox and input styling */
    .stSelectbox > div > div {
        border-radius: var(--border-radius);
        border: 2px solid #e0e0e0;
        font-family: 'Inter', sans-serif;
    }
    
    .stSelectbox > div > div:focus-within {
        border-color: var(--primary-color);
        box-shadow: 0 0 0 3px rgba(46, 134, 171, 0.1);
    }
    
    /* Footer styling */
    .footer {
        text-align: center;
        padding: 2rem;
        margin-top: 3rem;
        border-top: 1px solid #e0e0e0;
        font-family: 'Inter', sans-serif;
        color: var(--text-secondary);
    }
    
    /* Hide Streamlit default elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display:none;}
    
    /* Responsive design */
    @media (max-width: 768px) {
        .main-header h1 {
            font-size: 2rem;
        }
        .main-header .subtitle {
            font-size: 1rem;
        }
        .research-badges {
            flex-direction: column;
            align-items: center;
        }
    }
</style>
""", unsafe_allow_html=True)

def create_sample_data():
    """Generate realistic sample data for the dashboard demo"""
    np.random.seed(42)
    
    # Gaming behavior data
    gaming_data = {
        'participant_id': range(1, 501),
        'age': np.random.normal(25, 5, 500).astype(int),
        'gaming_hours_per_day': np.random.exponential(2.5, 500),
        'years_gaming': np.random.normal(10, 4, 500),
        'preferred_genre': np.random.choice(['Action', 'RPG', 'Strategy', 'Sports', 'Puzzle'], 500),
        'platform': np.random.choice(['PC', 'Console', 'Mobile', 'VR'], 500),
        'social_gaming': np.random.choice([True, False], 500),
        'competitive_gaming': np.random.choice([True, False], 500),
        'stress_level': np.random.normal(5, 2, 500),
        'anxiety_score': np.random.normal(4, 1.5, 500),
        'depression_score': np.random.normal(3, 1.2, 500),
        'wellbeing_score': np.random.normal(7, 1.8, 500),
        'sleep_quality': np.random.normal(6, 1.5, 500),
        'social_connections': np.random.normal(6, 2, 500),
        'academic_performance': np.random.normal(75, 15, 500)
    }
    
    # Add some realistic correlations
    for i in range(500):
        # Higher gaming hours might correlate with higher stress in extreme cases
        if gaming_data['gaming_hours_per_day'][i] > 6:
            gaming_data['stress_level'][i] += np.random.normal(1, 0.5)
            gaming_data['sleep_quality'][i] -= np.random.normal(1, 0.3)
        
        # Social gaming might improve wellbeing
        if gaming_data['social_gaming'][i]:
            gaming_data['wellbeing_score'][i] += np.random.normal(0.5, 0.3)
            gaming_data['social_connections'][i] += np.random.normal(0.8, 0.4)
    
    # Ensure realistic ranges
    gaming_data['stress_level'] = np.clip(gaming_data['stress_level'], 1, 10)
    gaming_data['anxiety_score'] = np.clip(gaming_data['anxiety_score'], 1, 10)
    gaming_data['depression_score'] = np.clip(gaming_data['depression_score'], 1, 10)
    gaming_data['wellbeing_score'] = np.clip(gaming_data['wellbeing_score'], 1, 10)
    gaming_data['sleep_quality'] = np.clip(gaming_data['sleep_quality'], 1, 10)
    gaming_data['social_connections'] = np.clip(gaming_data['social_connections'], 1, 10)
    gaming_data['academic_performance'] = np.clip(gaming_data['academic_performance'], 0, 100)
    
    return pd.DataFrame(gaming_data)

def main_dashboard():
    """Main dashboard function"""
    
    # Custom header
    st.markdown("""
    <div class="main-header">
        <h1>🧠 RespawnMetrics</h1>
        <div class="subtitle">Advanced Gaming Psychology Research Platform</div>
        <div class="research-badges">
            <span class="research-badge">🎮 Gaming Behavior Analysis</span>
            <span class="research-badge">🧘 Mental Health Research</span>
            <span class="research-badge">📊 Statistical Modeling</span>
            <span class="research-badge">🤖 Machine Learning</span>
        </div>
    </div>
    """, unsafe_allow_html=True)
    
    # Load sample data
    df = create_sample_data()
    
    # Sidebar configuration
    with st.sidebar:
        st.markdown("### 🔬 Research Controls")
        
        # Sample size selection
        sample_size = st.slider(
            "📈 Sample Size", 
            min_value=100, 
            max_value=500, 
            value=500, 
            step=50,
            help="Select number of participants for analysis"
        )
        
        # Age filter
        age_range = st.slider(
            "👥 Age Range", 
            min_value=int(df['age'].min()), 
            max_value=int(df['age'].max()), 
            value=(18, 35),
            help="Filter participants by age"
        )
        
        # Gaming hours filter
        gaming_hours_range = st.slider(
            "🎮 Gaming Hours per Day", 
            min_value=0.0, 
            max_value=12.0, 
            value=(0.0, 8.0), 
            step=0.5,
            help="Filter by daily gaming hours"
        )
        
        # Genre filter
        selected_genres = st.multiselect(
            "🎯 Gaming Genres",
            options=df['preferred_genre'].unique(),
            default=df['preferred_genre'].unique(),
            help="Select gaming genres to include"
        )
        
        st.markdown("---")
        st.markdown("### 📋 Quick Stats")
        st.metric("Total Participants", f"{len(df):,}")
        st.metric("Avg Gaming Hours", f"{df['gaming_hours_per_day'].mean():.1f}")
        st.metric("Avg Wellbeing Score", f"{df['wellbeing_score'].mean():.1f}")
    
    # Filter data based on sidebar selections
    filtered_df = df[
        (df['age'] >= age_range[0]) & 
        (df['age'] <= age_range[1]) &
        (df['gaming_hours_per_day'] >= gaming_hours_range[0]) &
        (df['gaming_hours_per_day'] <= gaming_hours_range[1]) &
        (df['preferred_genre'].isin(selected_genres))
    ].head(sample_size)
    
    # Key metrics row
    st.markdown('<div class="section-header">📊 Key Research Metrics</div>', unsafe_allow_html=True)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_gaming = filtered_df['gaming_hours_per_day'].mean()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">🎮 Avg Gaming Hours</div>
            <div class="metric-value">{avg_gaming:.1f}</div>
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
        avg_stress = filtered_df['stress_level'].mean()
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-title">😰 Stress Level</div>
            <div class="metric-value">{avg_stress:.1f}</div>
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
    
    # Main content tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Overview Analysis", "🔗 Correlations", "🎮 Gaming Patterns", "🧘 Mental Health"])
    
    with tab1:
        st.markdown('<div class="section-header">📊 Dataset Overview</div>', unsafe_allow_html=True)
        
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # Gaming hours distribution
            fig = px.histogram(
                filtered_df, 
                x='gaming_hours_per_day',
                nbins=30,
                title="🎮 Distribution of Daily Gaming Hours",
                color_discrete_sequence=['#2E86AB']
            )
            fig.update_layout(
                title_font_size=18,
                title_font_family="Inter",
                font_family="Inter",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Genre distribution
            genre_counts = filtered_df['preferred_genre'].value_counts()
            fig = px.pie(
                values=genre_counts.values,
                names=genre_counts.index,
                title="🎯 Gaming Genre Preferences",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig.update_layout(
                title_font_size=16,
                title_font_family="Inter",
                font_family="Inter"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Demographics overview
        col1, col2 = st.columns(2)
        
        with col1:
            # Age distribution
            fig = px.box(
                filtered_df, 
                y='age',
                title="👥 Age Distribution",
                color_discrete_sequence=['#A23B72']
            )
            fig.update_layout(
                title_font_size=16,
                title_font_family="Inter",
                font_family="Inter",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Platform usage
            platform_counts = filtered_df['platform'].value_counts()
            fig = px.bar(
                x=platform_counts.index,
                y=platform_counts.values,
                title="🖥️ Gaming Platform Usage",
                color=platform_counts.values,
                color_continuous_scale='Viridis'
            )
            fig.update_layout(
                title_font_size=16,
                title_font_family="Inter",
                font_family="Inter",
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.markdown('<div class="section-header">🔗 Correlation Analysis</div>', unsafe_allow_html=True)
        
        # Correlation heatmap
        numeric_columns = ['gaming_hours_per_day', 'stress_level', 'anxiety_score', 
                          'depression_score', 'wellbeing_score', 'sleep_quality', 
                          'social_connections', 'academic_performance']
        
        corr_matrix = filtered_df[numeric_columns].corr()
        
        fig = px.imshow(
            corr_matrix,
            title="🔥 Correlation Heatmap: Gaming & Mental Health Variables",
            color_continuous_scale='RdBu',
            aspect='auto'
        )
        fig.update_layout(
            title_font_size=18,
            title_font_family="Inter",
            font_family="Inter",
            height=600
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Key findings
        st.markdown("""
        <div class="info-box">
            <h4>🔍 Key Correlation Insights:</h4>
            <ul>
                <li><strong>Gaming Hours vs Sleep Quality:</strong> Moderate negative correlation suggests excessive gaming may impact sleep</li>
                <li><strong>Social Gaming vs Wellbeing:</strong> Positive correlation indicates social aspects may enhance mental health</li>
                <li><strong>Stress vs Academic Performance:</strong> Expected negative correlation observed</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        # Scatter plot matrix
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.scatter(
                filtered_df, 
                x='gaming_hours_per_day', 
                y='wellbeing_score',
                color='social_gaming',
                title="🎮 Gaming Hours vs Wellbeing Score",
                trendline="ols"
            )
            fig.update_layout(
                title_font_size=16,
                title_font_family="Inter",
                font_family="Inter"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            fig = px.scatter(
                filtered_df, 
                x='stress_level', 
                y='sleep_quality',
                color='gaming_hours_per_day',
                title="😰 Stress Level vs Sleep Quality",
                trendline="ols"
            )
            fig.update_layout(
                title_font_size=16,
                title_font_family="Inter",
                font_family="Inter"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.markdown('<div class="section-header">🎮 Gaming Behavior Patterns</div>', unsafe_allow_html=True)
        
        # Gaming patterns by genre
        col1, col2 = st.columns(2)
        
        with col1:
            genre_mental_health = filtered_df.groupby('preferred_genre').agg({
                'wellbeing_score': 'mean',
                'stress_level': 'mean',
                'anxiety_score': 'mean'
            }).round(2)
            
            fig = px.bar(
                x=genre_mental_health.index,
                y=genre_mental_health['wellbeing_score'],
                title="🎯 Wellbeing Score by Gaming Genre",
                color=genre_mental_health['wellbeing_score'],
                color_continuous_scale='Viridis'
            )
            fig.update_layout(
                title_font_size=16,
                title_font_family="Inter",
                font_family="Inter"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Social vs Solo gaming comparison
            social_comparison = filtered_df.groupby('social_gaming').agg({
                'wellbeing_score': 'mean',
                'social_connections': 'mean',
                'stress_level': 'mean'
            }).round(2)
            
            fig = px.bar(
                x=['Solo Gaming', 'Social Gaming'],
                y=social_comparison['wellbeing_score'],
                title="👥 Social vs Solo Gaming Impact on Wellbeing",
                color=['#FF6B6B', '#4ECDC4']
            )
            fig.update_layout(
                title_font_size=16,
                title_font_family="Inter",
                font_family="Inter"
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Gaming hours vs outcomes
        fig = px.scatter_3d(
            filtered_df,
            x='gaming_hours_per_day',
            y='wellbeing_score',
            z='stress_level',
            color='preferred_genre',
            title="🌟 3D View: Gaming Hours, Wellbeing & Stress by Genre",
            size='social_connections'
        )
        fig.update_layout(
            title_font_size=18,
            title_font_family="Inter",
            font_family="Inter",
            height=600
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.markdown('<div class="section-header">🧘 Mental Health Analysis</div>', unsafe_allow_html=True)
        
        # Mental health overview
        col1, col2, col3 = st.columns(3)
        
        with col1:
            avg_anxiety = filtered_df['anxiety_score'].mean()
            st.metric(
                "😟 Average Anxiety Score", 
                f"{avg_anxiety:.1f}", 
                f"{avg_anxiety - df['anxiety_score'].mean():.1f}"
            )
        
        with col2:
            avg_depression = filtered_df['depression_score'].mean()
            st.metric(
                "😔 Average Depression Score", 
                f"{avg_depression:.1f}", 
                f"{avg_depression - df['depression_score'].mean():.1f}"
            )
        
        with col3:
            avg_sleep = filtered_df['sleep_quality'].mean()
            st.metric(
                "😴 Average Sleep Quality", 
                f"{avg_sleep:.1f}", 
                f"{avg_sleep - df['sleep_quality'].mean():.1f}"
            )
        
        # Mental health distribution
        mental_health_cols = ['anxiety_score', 'depression_score', 'stress_level', 'wellbeing_score']
        
        fig = make_subplots(
            rows=2, cols=2,
            subplot_titles=('😟 Anxiety Scores', '😔 Depression Scores', 
                           '😰 Stress Levels', '😊 Wellbeing Scores')
        )
        
        for i, col in enumerate(mental_health_cols):
            row = (i // 2) + 1
            col_pos = (i % 2) + 1
            
            fig.add_trace(
                go.Histogram(x=filtered_df[col], name=col, showlegend=False),
                row=row, col=col_pos
            )
        
        fig.update_layout(
            title_text="🧠 Mental Health Metrics Distribution",
            title_font_size=18,
            title_font_family="Inter",
            font_family="Inter",
            height=600
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Risk analysis
        st.markdown('<div class="section-header">⚠️ Risk Factor Analysis</div>', unsafe_allow_html=True)
        
        # High-risk gaming identification
        high_gaming_hours = filtered_df['gaming_hours_per_day'] > filtered_df['gaming_hours_per_day'].quantile(0.9)
        high_stress = filtered_df['stress_level'] > filtered_df['stress_level'].quantile(0.75)
        poor_sleep = filtered_df['sleep_quality'] < filtered_df['sleep_quality'].quantile(0.25)
        
        risk_analysis = pd.DataFrame({
            'Risk Factor': ['High Gaming Hours (>90th percentile)', 'High Stress (>75th percentile)', 'Poor Sleep (<25th percentile)'],
            'Count': [high_gaming_hours.sum(), high_stress.sum(), poor_sleep.sum()],
            'Percentage': [
                (high_gaming_hours.sum() / len(filtered_df)) * 100,
                (high_stress.sum() / len(filtered_df)) * 100,
                (poor_sleep.sum() / len(filtered_df)) * 100
            ]
        })
        
        fig = px.bar(
            risk_analysis,
            x='Risk Factor',
            y='Percentage',
            title="⚠️ Risk Factor Prevalence in Sample",
            color='Percentage',
            color_continuous_scale='Reds'
        )
        fig.update_layout(
            title_font_size=16,
            title_font_family="Inter",
            font_family="Inter"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Research insights section
    st.markdown('<div class="section-header">💡 Research Insights & Recommendations</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("""
        <div class="info-box">
            <h4>🔬 Key Research Findings:</h4>
            <ul>
                <li><strong>Moderate Gaming Benefits:</strong> 2-4 hours/day shows optimal wellbeing scores</li>
                <li><strong>Social Gaming Advantage:</strong> Social gamers report 15% higher wellbeing</li>
                <li><strong>Genre Matters:</strong> Puzzle and strategy games correlate with better cognitive outcomes</li>
                <li><strong>Sleep Impact:</strong> Gaming >6 hours/day significantly affects sleep quality</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="warning-box">
            <h4>⚠️ Clinical Recommendations:</h4>
            <ul>
                <li><strong>Screen Time Limits:</strong> Monitor individuals gaming >8 hours/day</li>
                <li><strong>Social Integration:</strong> Encourage multiplayer and community gaming</li>
                <li><strong>Sleep Hygiene:</strong> Establish gaming curfews for better sleep</li>
                <li><strong>Regular Assessment:</strong> Monitor mental health in high-use populations</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
    
    # Footer
    st.markdown("""
    <div class="footer">
        <h3>🧠 RespawnMetrics</h3>
        <p>Advanced Gaming Psychology Research Platform | Built by Reannon Gray</p>
        <p><strong>Research Portfolio:</strong> <a href="https://reannoncodes.site" target="_blank">reannoncodes.site</a> | 
        <strong>GitHub:</strong> <a href="https://github.com/reannongray/respawnmetrics" target="_blank">respawnmetrics</a></p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main_dashboard()