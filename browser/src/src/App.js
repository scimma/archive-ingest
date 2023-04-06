import { Component } from "react";
import {
  Container,
  AppBar,
  Link,
  Box,
  Toolbar,
  Typography,
  IconButton,
  Grid,
  Button,
  ButtonGroup,
  Dialog,
  DialogActions,
  DialogContent,
  DialogContentText,
  DialogTitle,
} from '@mui/material';
import MenuIcon from '@mui/icons-material/Menu';
import { createTheme, ThemeProvider } from '@mui/material/styles';
import { DataGrid } from '@mui/x-data-grid';
import { withStyles } from '@mui/styles';
import CssBaseline from '@mui/material/CssBaseline';
import config from "./app.config";
// import SelectTopic from './SelectTopic.jsx'


const theme = createTheme({
  palette: {
    primary: {
      // main: "#18381b"
      main: "#5daefd"
    },
    secondary: {
      main: "#e8a114"
    },
    third: {
      main: "#333333"
    },
    fourth: {
      main: "#eeeeee"
    }
  },
  typography: {
    h1: {
      fontFamily: "'Roboto Condensed',sans-serif"
    },
    h2: {
      fontFamily: "'Roboto', 'Helvetica', 'Arial', sans-serif"
    },
    h3: {
      fontFamily: "'Roboto', 'Helvetica', 'Arial', sans-serif"
    },
    h4: {
      fontFamily: "'Roboto', 'Helvetica', 'Arial', sans-serif"
    },
    h5: {
      fontFamily: "'Roboto', 'Helvetica', 'Arial', sans-serif",
      fontSize: { xs: '1rem', sm: '2rem' },
    },
    h6: {
      fontFamily: "'Roboto', 'Helvetica', 'Arial', sans-serif"
    },
    body1: {
      fontFamily: "'Work Sans',sans-serif",
      fontSize: "1rem"
    },
    body2: {
      fontFamily: "'Work Sans',sans-serif",
      fontSize: "1rem"
    }
  },
});

const styles = {
  menuCustomWidth: {
    "& li": {
      width: "200px"
    }
  },
  appBar: {
    width: "100%",
    transition: theme.transitions.create(["margin", "width"], {
      easing: theme.transitions.easing.sharp,
      duration: theme.transitions.duration.leavingScreen,
    }),
  },
  container: {
    "&": {
      backgroundColor: "#ffffff",
      marginTop: theme.spacing(3),
      marginBottom: theme.spacing(1),
      display: "flex",
      flexDirection: "column",
      alignItems: "center",
    },
  },
  messageTableContainer: {
    marginTop: theme.spacing(0),
  },
  avatarImg: {
    width: "32px",
    height: "32px",
    borderRadius: "50%",
    // border:"solid 2px #FFFFFF"
  },
  toolBar: {
    minHeight: "48px",
    display: "flex",
    justifyContent: "flex-start",
  },
  smallButton: {
    padding: "6px"
  },
  denseStyle: {
    minHeight: "10px",
    lineHeight: "30px",
    fontSize: "12px",
  },
  status: {
    padding: "6px 16px"
  },
  fontLight: {
    fontSize: "12px",
    color: "#333333",
    fontWeight: 100,
    fontFamily: theme.typography.body1.fontFamily,
  },
  fontBold: {
    fontSize: "12px",
    color: "#000000",
    fontWeight: 600,
    fontFamily: theme.typography.body1.fontFamily,
  },
  toolBarItem: {
    margin: "auto 20px",
    cursor: "pointer"
  },
  customProgressBar: {
    borderRadius: 5,
    height: 5,
  },
  bannerText: {
    "&:hover": {
      cursor: "pointer"
    },
  },
  selectedButton: {
    backgroundColor: "red",
  }
};

class App extends Component {

  constructor(props) {
    super(props);
    this.state = {
      topic: null,
      topics: [],
      messages: [],
      sortModel: [
        {
          field: 'timestamp',
          sort: 'desc',
        }],
      details: {
        open: false,
        id: null,
        bodyText: "",
      }
    }
    this.setTopic = this.setTopic.bind(this);
    this.getTopics = this.getTopics.bind(this);
    this.listTopic = this.listTopic.bind(this);
    this.setSortModel = this.setSortModel.bind(this);
    this.clickTopic = this.clickTopic.bind(this);
    this.closeDetails = this.closeDetails.bind(this);
  }

  async listTopic() {
    const endpoint = `${config.protocol}://${config.hostname}:${config.port}${config.baseApiUrl}/topic/${this.state.topic}`;
    // const payload = {
    //   param1: this.state.topic,
    //   param2: "param1 is the selected topic",
    // };
    let response = await fetch(endpoint, {
      method: "GET",
      // headers: {
      //   "Content-Type": "application/json",
      // },
      // body: JSON.stringify(payload)
    });
    // this.handleResponselistTopic(response)
    if (response.status >= 200 && response.status <= 299) {
      let data = await response.json();
      // console.log(JSON.stringify(data, null, 2));
      let messages = data.messages.map((message, index, array) => {
        return {
          id: message['uuid'],
          timestamp: new Date(message['timestamp']),
          key: message['key'],
        }
      });
      this.setState({
        messages: messages,
      })
    } else {
      console.log(JSON.stringify(response.statusText, null, 2));
    }
  }

  async getTopics() {
    const endpoint = `${config.protocol}://${config.hostname}:${config.port}${config.baseApiUrl}/topics`;
    let response = await fetch(endpoint, {
      method: "GET",
    });
    if (response.status >= 200 && response.status <= 299) {
      let data = await response.json();
      // console.log(JSON.stringify(data, null, 2));
      this.setState({
        topics: data['topics'],
      })
    } else {
      console.log(JSON.stringify(response.statusText, null, 2));
    }
  }

  setSortModel(model) {
    this.setState({
      sortModel: model,
    })
  };

  columns = [
    {
      field: 'timestamp',
      headerName: 'Time',
      description: 'Time stamp (UTC)',
      type: 'dateTime',
      sortable: true,
      flex: 1,
      minWidth: 50,
    },
    {
      field: 'id',
      headerName: 'ID',
      description: 'Unique message ID',
      sortable: false,
      flex: 1,
      minWidth: 50
    },
    {
      field: 'key',
      headerName: 'Key',
      description: 'Message key',
      sortable: true,
      flex: 1,
      minWidth: 50
    },
  ]

  componentDidMount() {
    this.getTopics()
  }

  clickTopic = (event) => {
    event.preventDefault();
    this.setTopic(event.target.value)
  }

  setTopic(selectedTopic) {
    this.setState({
      topic: selectedTopic
    }, () => {
      // console.log(`topic: ${selectedTopic}`)
      this.listTopic()
    })
  }

	async fetchDetails(id) {
    const endpoint = `${config.protocol}://${config.hostname}:${config.port}${config.baseApiUrl}/message/${id}`;
    let response = await fetch(endpoint, {
      method: "GET",
    });
    if (response.status >= 200 && response.status <= 299) {
      let data = await response.json();
      // console.log( JSON.stringify(data, null, 2));
      let metadataJson = JSON.stringify(data.details.metadata, null, 2)
      // console.log(metadataJson);
      let messageJson = JSON.stringify(data.details.message, null, 2)
      // console.log(messageJson);
      this.setState({
        details: {
          // ...details,
          id: id,
          bodyText: `metadata:\n${metadataJson}\n\nmessage:\n${messageJson}`,
          open: true,
        }
      })
    } else {
      console.log(JSON.stringify(response.statusText, null, 2));
    }
  }

  closeDetails() {
    this.setState({
      details: {
        id: null,
        bodyText: "",
        open: false,
      }
    })
  }

  render() {
    const { classes } = this.props;
    const drawerWidth = 240;
    let selectedTopic = <></>;
    if (this.state.topic !== null) {
      selectedTopic = (
        <Typography>
          Downloading messages in topic <Typography variant="bold">{this.state.topic}</Typography>...
        </Typography>
      )
    }
    let topicButtons = [];
    for (var i = 0; i < this.state.topics.length; i++) {
      let topic = this.state.topics[i]
      topicButtons.push(
        <Button key={topic} value={topic} className={key === this.state.topic ? classes.selectedButton : ''} onClick={this.clickTopic}>{topic}</Button>
      )
    }
    return (
      <ThemeProvider theme={theme}>
        <CssBaseline />
        <Box sx={{ display: 'flex' }}>

          <AppBar position="fixed"
            sx={{
              zIndex: (theme) => theme.zIndex.drawer + 1,
              // width: { sm: `calc(100% - ${drawerWidth}px)` },
              // ml: { sm: `${drawerWidth}px` },
            }}
            className={classes.appBar}>
            <Toolbar className={classes.toolBar}>
              <IconButton
                color="inherit"
                aria-label="open drawer"
                edge="start"
                // onClick={this.handleDrawerToggle}
                sx={{ mr: 2, display: { sm: 'none' } }}
              >
                <MenuIcon />
              </IconButton>
              <Link className={classes.bannerText} style={{ color: "#ffffff", textDecoration: "none" }}
                sx={{
                  fontSize: { xs: '1rem', sm: '1.5rem' },
                }}
              // onClick={(event) => {
              // 	// event.stopPropagation();
              // 	browserHistory.push(`${config.baseUrl}`);
              // }}
              >
                <Typography variant="h5">
                  SCiMMA // Archive Browser
                </Typography>
              </Link>
              <Typography variant="body1" style={{ flex: 1 }} />
            </Toolbar>
          </AppBar>
          <Box
            component="main"
            sx={{ flexGrow: 1, p: 3, width: { xs: '100%', sm: `calc(100% - ${drawerWidth}px)` } }}
          >

            <Container className={classes.container}>
              <Grid container spacing={4} marginTop={0} >
                <Grid item sm={12} md={3}>
                  <Typography>
                    Select a topic to browse the SCiMMA Hopskotch message archive.
                  </Typography>
                </Grid>
                <Grid item sm={12} md={9}>
                  {/* <SelectTopic setTopic={this.setTopic} topics={this.state.topics} topic={this.state.topic} /> */}
                  <Box
                    sx={{
                      display: 'flex',
                      '& > *': {
                        m: 1,
                      },
                    }}
                  >
                    <ButtonGroup
                      orientation="horizontal"
                      aria-label="horizontal outlined button group"
                      variant="outlined"
                    >
                      {topicButtons}
                    </ButtonGroup>
                  </Box>
                </Grid>
              </Grid>
              <Grid container spacing={4} marginTop={2} >
                <Grid item md={12}>
                  {selectedTopic}
                </Grid>
              </Grid>
            </Container>
            <Container className={classes.messageTableContainer}>
                <DataGrid
                  sx={{
                    height: '60vh',
                    width: '100%',
                  }}
                  rows={this.state.messages}
                  columns={this.columns}
                  // autoHeight
                  autoPageSize={false}
                  pageSizeOptions={[20, 50, 100]}
                  onRowClick={(params, event, details) => {this.fetchDetails(params.id)}}
                  onSortModelChange={(model) => this.setSortModel(model)}
                  sortModel={this.state.sortModel}
                />
                      
              <Dialog
                open={this.state.details.open}
                onClose={this.closeDetails}
                scroll={'paper'}
                aria-labelledby="scroll-dialog-title"
                aria-describedby="scroll-dialog-description"
                fullWidth={true}
                maxWidth="lg"
              >
                <DialogTitle id="scroll-dialog-title">Message ID: <code>{this.state.details.id}</code></DialogTitle>
                <DialogContent dividers={true}>
                  <DialogContentText
                    id="scroll-dialog-description"
                    // ref={descriptionElementRef}
                    tabIndex={-1}
                  >
                    <pre sx={{whiteSpace: "pre-wrap"}}>{this.state.details.bodyText}</pre>
                  </DialogContentText>
                </DialogContent>
                <DialogActions>
                  <Button onClick={this.closeDetails}>Close</Button>
                </DialogActions>
              </Dialog>
            </Container>
          </Box>
        </Box>
      </ThemeProvider>
    );
  }
}

export default withStyles(styles)(App);
